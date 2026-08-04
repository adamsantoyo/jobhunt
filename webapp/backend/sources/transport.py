"""The concrete HTTP transport the scheduler hands to adapters.

Why httpx: the contract is an async stream, so a sync client is disqualified
(`requests` would need a thread per in-flight request, forfeiting the bounded
per-host connection pooling that Phase 2.3's limits are expressed in). httpx is
already in the locked environment as a dev dependency (`uv.lock`), it is the
client Starlette's own test client uses, and its `Limits` maps directly onto
`SourceDescriptor.per_host_concurrency`. Promoting it from `dependency-groups.dev`
to `project.dependencies` is the one packaging change Phase 2 requires; it is a
promotion of an already-locked package, not a new dependency. The import is
deferred to `__init__` so that `backend.sources` — including every pure parser
and the whole contract — imports and tests cleanly without httpx present.

This module owns exactly three policies, and adapters own none of them:
  * the socket timeout (connect/read/write/pool),
  * the connection pool and its per-host ceiling,
  * TLS verification (`JOBHUNT_INSECURE=1` disables it, matching `scraper.py`).

It does NOT retry and it does NOT enforce the run deadline. One `send` is one
attempt; the scheduler wraps stream consumption in its own timeout and decides
whether to spend the single permitted retry.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Mapping
from typing import Any

from .contract import (
    HttpRequest,
    HttpResponse,
    PermanentSourceError,
    TransientSourceError,
    Transport,
)

__all__ = ["DEFAULT_HEADERS", "HttpxTransport", "PacedTransport", "verify_tls"]

#: Matches `scraper.py`'s header set. Public ATS endpoints reject the default
#: python-httpx agent often enough that this is load-bearing, not cosmetic.
DEFAULT_HEADERS: Mapping[str, str] = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
}


def verify_tls() -> bool:
    """TLS verification is on unless `JOBHUNT_INSECURE` says otherwise.

    Preserves the existing escape hatch for sandboxes behind an intercepting
    proxy, with the same env var and the same default (on).
    """
    return os.environ.get("JOBHUNT_INSECURE", "").lower() not in ("1", "true")


class HttpxTransport:
    """`Transport` backed by one shared `httpx.AsyncClient`.

    Constructed once per run by the scheduler and shared by every adapter, so
    connection reuse applies across sources rather than per source. Use as an async
    context manager, or call `aclose()`.

    KNOWN LIMITATION, stated rather than worked around: httpx has no per-host
    connection limit. `httpx.Limits` offers `max_connections` (pool-wide) and
    `max_keepalive_connections` (pool-wide idle sockets kept alive); neither is
    per-host, and `max_connections_per_host` below is mapped onto the keepalive cap,
    which is a reuse hint and not a ceiling. Emulating a real per-host limit here
    would mean intercepting the pool, so the per-host ceiling that the descriptors
    actually promise is enforced one layer up, in `PacedTransport`, where it is one
    semaphore keyed by request host and is testable without a socket. Treat the
    argument below as pool tuning; `SourceDescriptor.per_host_concurrency` is the
    ceiling that binds.
    """

    def __init__(
        self,
        *,
        timeout: float = 15.0,
        max_connections: int = 32,
        #: Pool-wide keepalive cap, NOT a per-host limit — see the class docstring.
        max_connections_per_host: int = 6,
        headers: Mapping[str, str] | None = None,
        verify: bool | None = None,
        follow_redirects: bool = True,
        client: Any = None,
    ) -> None:
        if client is not None:
            self._client = client
            self._owns_client = False
            return
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - environment guard
            raise RuntimeError(
                "HttpxTransport requires httpx. Install it, or pass a pre-built client, "
                "or use a different Transport implementation."
            ) from exc
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections_per_host,
            ),
            headers=dict(headers or DEFAULT_HEADERS),
            verify=verify_tls() if verify is None else verify,
            follow_redirects=follow_redirects,
        )
        self._owns_client = True

    async def __aenter__(self) -> HttpxTransport:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def send(self, request: HttpRequest) -> HttpResponse:
        """Perform one request. Any status comes back as a response.

        Only transport-level failures raise, and they raise already classified
        so the scheduler never has to inspect an httpx type. Status-level
        decisions belong to the adapter via `check_status`, because "404 means
        empty" is true for some endpoints and a broken-source signal for others.
        """
        import httpx

        try:
            response = await self._client.request(
                request.method,
                request.url,
                params=dict(request.params) if request.params else None,
                headers=dict(request.headers) if request.headers else None,
                json=request.json_body,
            )
        except asyncio.CancelledError:
            # The scheduler's deadline or a cancelled run. Never reclassify it:
            # swallowing CancelledError here would make a source untimeoutable.
            raise
        except (httpx.InvalidURL, httpx.UnsupportedProtocol) as exc:
            raise PermanentSourceError(
                f"invalid request url: {exc}", url=request.url
            ) from exc
        except httpx.HTTPError as exc:
            # Timeouts, connection resets, protocol errors, pool exhaustion.
            # All plausibly transient, and the scheduler allows at most one
            # retry, so an occasional wasted retry is cheaper than treating a
            # blip as a dead source.
            raise TransientSourceError(
                f"{type(exc).__name__}: {exc}", url=request.url
            ) from exc
        return HttpResponse(
            status=response.status_code,
            url=str(response.url),
            content=response.content,
            headers={k.lower(): v for k, v in response.headers.items()},
        )


class PacedTransport:
    """Wraps a transport with per-host politeness and a per-host concurrency cap.

    This is where `SourceDescriptor.min_request_interval_seconds` and
    `per_host_concurrency` are actually enforced, which is why adapters are
    forbidden from sleeping: the 0.2s pauses scattered through `scraper.py`
    become one policy applied by the scheduler, visible in the deadline budget
    and adjustable without touching sixteen adapters.

    The wait happens outside the adapter's control flow, so a source waiting
    its turn is still cancellable at the run's deadline.

    One instance per host (the scheduler builds them that way), which is what makes
    the state below meaningful: an instance shared by two sources behind one host
    paces them together, and two instances on one host would each pace half of a
    host that experiences all of it.
    """

    def __init__(
        self,
        inner: Transport,
        *,
        min_interval_seconds: float = 0.0,
        per_host_concurrency: int = 4,
    ) -> None:
        self._inner = inner
        self._min_interval = max(0.0, min_interval_seconds)
        self._per_host = max(1, per_host_concurrency)
        self._gates: dict[str, asyncio.Semaphore] = {}
        #: Per host, the earliest instant at which the NEXT request may be sent.
        #: A schedule of future slots rather than a memory of the last send: see
        #: `_reserve` for why the difference is the whole feature.
        self._next_at: dict[str, float] = {}

    def _gate(self, host: str) -> asyncio.Semaphore:
        gate = self._gates.get(host)
        if gate is None:
            gate = asyncio.Semaphore(self._per_host)
            self._gates[host] = gate
        return gate

    async def _reserve(self, host: str) -> None:
        """Claim this host's next send slot, then wait for it to arrive.

        The claim is taken and the following slot written BEFORE the wait, with no
        await in between, so two concurrent callers cannot claim the same instant.
        Reading a "last request at" stamp instead — which is what this did before
        2.6 — makes interval pacing a no-op as soon as `per_host_concurrency > 1`:
        every caller admitted by the gate reads the same stamp, sleeps the same
        remainder, and they all hit the host together, which is precisely the
        stampede the interval exists to prevent.

        The slot is also claimed at request START rather than stamped at completion,
        so the interval is the gap between requests arriving at the host rather than
        a gap appended to however long the host took to answer.
        """
        if not self._min_interval:
            return
        now = time.monotonic()
        at = max(now, self._next_at.get(host, 0.0))
        self._next_at[host] = at + self._min_interval
        if at > now:
            await asyncio.sleep(at - now)

    async def send(self, request: HttpRequest) -> HttpResponse:
        host = request.host
        async with self._gate(host):
            await self._reserve(host)
            return await self._inner.send(request)
