"""Fake transport, fixture loading, and stream helpers for adapter tests.

Ships inside the package rather than under `tests/` on purpose: all sixteen
adapters are tested the same way, and CI is required to run on frozen fixtures
with no live network. Anything an adapter test needs should live here so the
sixteen test modules stay thin.
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .contract import (
    FetchContext,
    HttpRequest,
    HttpResponse,
    NormalizedPosting,
    SourceAdapter,
    SourceTarget,
)

__all__ = [
    "FIXTURE_ROOT",
    "FakeTransport",
    "collect",
    "drain",
    "fixture_bytes",
    "fixture_json",
    "fixture_path",
    "json_response",
    "text_response",
]

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"


# --------------------------------------------------------------------------- #
# Frozen fixtures
# --------------------------------------------------------------------------- #
def fixture_path(source_key: str, name: str) -> Path:
    """`fixtures/<source_key>/<name>` — one directory per source."""
    path = FIXTURE_ROOT / source_key / name
    if not path.is_file():
        raise FileNotFoundError(f"no frozen fixture at {path}")
    return path


def fixture_bytes(source_key: str, name: str) -> bytes:
    """Raw fixture bytes, exactly as the transport would have returned them.

    Bytes, not a parsed object, so the parser under test does its own decoding
    and encoding bugs cannot hide behind the fixture loader.
    """
    return fixture_path(source_key, name).read_bytes()


def fixture_json(source_key: str, name: str) -> Any:
    return json.loads(fixture_bytes(source_key, name))


# --------------------------------------------------------------------------- #
# Fake transport
# --------------------------------------------------------------------------- #
Responder = Callable[[HttpRequest], HttpResponse]


def json_response(payload: Any, *, status: int = 200, url: str = "") -> HttpResponse:
    return HttpResponse(
        status=status,
        url=url,
        content=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
    )


def text_response(body: str, *, status: int = 200, url: str = "") -> HttpResponse:
    return HttpResponse(
        status=status,
        url=url,
        content=body.encode("utf-8"),
        headers={"content-type": "text/html; charset=utf-8"},
    )


@dataclass
class FakeTransport:
    """Programmable in-memory `Transport`.

    Routes are keyed on the URL with query and fragment stripped, since
    adapters pass query data as `HttpRequest.params`. A route's value is either
    a single response/exception, or a list consumed one per call with the last
    entry repeating — which is how a paginated source is faked (page 1, page 2,
    then an empty page forever). A `Callable` value receives the request, so a
    test can branch on `params`.

    Every request is appended to `requests`, so tests can assert both call
    count (proving an adapter did not fetch a page it did not need) and
    ordering.
    """

    routes: dict[str, Any] = field(default_factory=dict)
    default: Any = None
    requests: list[HttpRequest] = field(default_factory=list)

    @staticmethod
    def route_key(url: str) -> str:
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    def add(self, url: str, *responses: Any) -> FakeTransport:
        """Queue one or more responses (or exceptions) for a URL."""
        self.routes[self.route_key(url)] = list(responses) if len(responses) != 1 else responses[0]
        return self

    def add_json(self, url: str, *payloads: Any, status: int = 200) -> FakeTransport:
        return self.add(url, *[json_response(p, status=status, url=url) for p in payloads])

    async def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        entry = self.routes.get(self.route_key(request.url), self.default)
        if entry is None:
            raise AssertionError(f"FakeTransport has no route for {request.url!r}")
        if isinstance(entry, list):
            entry = entry[0] if len(entry) == 1 else entry.pop(0)
        if callable(entry) and not isinstance(entry, HttpResponse):
            entry = entry(request)
        if isinstance(entry, BaseException):
            raise entry
        if isinstance(entry, type) and issubclass(entry, BaseException):
            raise entry("FakeTransport programmed failure")
        if not isinstance(entry, HttpResponse):
            raise AssertionError(f"FakeTransport route produced {type(entry).__name__}, not HttpResponse")
        return entry if entry.url else HttpResponse(
            status=entry.status, url=request.url, content=entry.content, headers=entry.headers
        )

    @property
    def call_count(self) -> int:
        return len(self.requests)

    def urls(self) -> list[str]:
        return [r.url for r in self.requests]


# --------------------------------------------------------------------------- #
# Stream helpers
# --------------------------------------------------------------------------- #
async def drain(stream: AsyncIterator[NormalizedPosting]) -> list[NormalizedPosting]:
    return [record async for record in stream]


async def collect(
    adapter: SourceAdapter, target: SourceTarget, ctx: FetchContext
) -> list[NormalizedPosting]:
    """Run one target to exhaustion. Convenience only — real consumers stream."""
    return await drain(adapter.fetch(target, ctx))


def assert_targets(targets: Sequence[SourceTarget], expected_instances: Mapping[str, str]) -> None:
    """Assert a `plan()` produced exactly these instance keys and labels."""
    got = {t.instance_key: t.label for t in targets}
    assert got == dict(expected_instances), f"planned {got}, expected {dict(expected_instances)}"
