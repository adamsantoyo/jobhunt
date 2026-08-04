"""The httpx transport seam: error classification and per-host pacing.

No sockets are opened. `HttpxTransport` is driven with a stub client, which is
the whole reason the seam exists — the scheduler's retry policy depends on
transport failures arriving already classified, and that mapping must be
testable without a network.
"""
import asyncio

import httpx
import pytest

from backend.sources.contract import (
    HttpRequest,
    HttpResponse,
    PermanentSourceError,
    TransientSourceError,
)
from backend.sources.transport import DEFAULT_HEADERS, HttpxTransport, PacedTransport, verify_tls


class StubClient:
    """Minimal stand-in for `httpx.AsyncClient`."""

    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    async def request(self, method, url, params=None, headers=None, json=None):
        self.calls.append((method, url, params, headers, json))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome

    async def aclose(self):  # pragma: no cover - never called for borrowed clients
        raise AssertionError("a borrowed client must not be closed by the transport")


def _httpx_response(status=200, body=b'{"jobs": []}'):
    return httpx.Response(
        status_code=status,
        content=body,
        headers={"Content-Type": "application/json"},
        request=httpx.Request("GET", "https://x.example/a"),
    )


def test_successful_request_is_returned_verbatim_at_any_status():
    """4xx/5xx come back as responses; only the adapter decides what they mean."""
    for status in (200, 404, 503):
        transport = HttpxTransport(client=StubClient(_httpx_response(status=status)))
        response = asyncio.run(transport.send(HttpRequest(url="https://x.example/a")))
        assert isinstance(response, HttpResponse)
        assert response.status == status
        assert response.content == b'{"jobs": []}'
        assert response.headers["content-type"] == "application/json"


def test_request_passes_method_params_headers_and_body_through():
    stub = StubClient(_httpx_response())
    transport = HttpxTransport(client=stub)
    asyncio.run(
        transport.send(
            HttpRequest(
                url="https://x.example/a",
                method="POST",
                params={"page": 1},
                headers={"X-Test": "1"},
                json_body={"searchText": "support engineer"},
            )
        )
    )
    method, url, params, headers, body = stub.calls[0]
    assert (method, url) == ("POST", "https://x.example/a")
    assert params == {"page": 1}
    assert headers == {"X-Test": "1"}
    assert body == {"searchText": "support engineer"}


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectTimeout("timed out"),
        httpx.ReadTimeout("timed out"),
        httpx.PoolTimeout("pool exhausted"),
        httpx.ConnectError("refused"),
        httpx.RemoteProtocolError("bad chunk"),
    ],
)
def test_network_failures_are_classified_transient(exc):
    transport = HttpxTransport(client=StubClient(exc))
    with pytest.raises(TransientSourceError) as raised:
        asyncio.run(transport.send(HttpRequest(url="https://x.example/a")))
    assert raised.value.url == "https://x.example/a"


@pytest.mark.parametrize(
    "exc", [httpx.InvalidURL("nope"), httpx.UnsupportedProtocol("gopher://x")]
)
def test_malformed_requests_are_classified_permanent(exc):
    transport = HttpxTransport(client=StubClient(exc))
    with pytest.raises(PermanentSourceError):
        asyncio.run(transport.send(HttpRequest(url="https://x.example/a")))


def test_cancellation_is_never_reclassified():
    """Reclassifying CancelledError would make a source impossible to time out."""
    transport = HttpxTransport(client=StubClient(asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(transport.send(HttpRequest(url="https://x.example/a")))


def test_borrowed_client_is_not_closed_by_the_transport():
    transport = HttpxTransport(client=StubClient(_httpx_response()))
    asyncio.run(transport.aclose())  # StubClient.aclose would raise if called


def test_verify_tls_defaults_on_and_honours_the_existing_escape_hatch(monkeypatch):
    monkeypatch.delenv("JOBHUNT_INSECURE", raising=False)
    assert verify_tls() is True
    monkeypatch.setenv("JOBHUNT_INSECURE", "1")
    assert verify_tls() is False
    monkeypatch.setenv("JOBHUNT_INSECURE", "0")
    assert verify_tls() is True


def test_default_headers_present():
    assert "Mozilla/5.0" in DEFAULT_HEADERS["User-Agent"]


# --------------------------------------------------------------------------- #
# PacedTransport
# --------------------------------------------------------------------------- #
class RecordingTransport:
    def __init__(self):
        self.in_flight = 0
        self.max_in_flight = 0
        self.hosts = []
        #: When each request reached the inner transport, which is what pacing is a
        #: statement about (arrivals at the host, not departures from it).
        self.started_at = []

    async def send(self, request):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        self.hosts.append(request.host)
        self.started_at.append(asyncio.get_running_loop().time())
        try:
            await asyncio.sleep(0)
            return HttpResponse(status=200, url=request.url)
        finally:
            self.in_flight -= 1


def test_paced_transport_caps_per_host_concurrency():
    inner = RecordingTransport()
    paced = PacedTransport(inner, per_host_concurrency=2)

    async def scenario():
        await asyncio.gather(
            *[paced.send(HttpRequest(url=f"https://one.example/{i}")) for i in range(8)]
        )

    asyncio.run(scenario())
    assert inner.max_in_flight <= 2
    assert len(inner.hosts) == 8


def test_paced_transport_does_not_serialize_across_different_hosts():
    inner = RecordingTransport()
    paced = PacedTransport(inner, per_host_concurrency=1)

    async def scenario():
        await asyncio.gather(
            paced.send(HttpRequest(url="https://one.example/a")),
            paced.send(HttpRequest(url="https://two.example/a")),
            paced.send(HttpRequest(url="https://three.example/a")),
        )

    asyncio.run(scenario())
    assert inner.max_in_flight > 1


def test_paced_transport_applies_the_minimum_interval_per_host():
    """Politeness lives here, so adapters never have to sleep (invariant 2)."""
    inner = RecordingTransport()
    paced = PacedTransport(inner, min_interval_seconds=0.05, per_host_concurrency=1)

    async def scenario():
        started = asyncio.get_running_loop().time()
        for i in range(3):
            await paced.send(HttpRequest(url=f"https://one.example/{i}"))
        return asyncio.get_running_loop().time() - started

    elapsed = asyncio.run(scenario())
    assert elapsed >= 0.09  # first request is free, the next two each wait


def test_interval_pacing_still_spaces_requests_when_the_host_allows_several():
    """Pacing must not evaporate the moment a host is allowed more than one slot.

    Reading a "last request at" stamp inside the gate is what made it evaporate: four
    callers admitted at once all read the same stamp, all slept the same remainder,
    and all hit the host together — an interval that paced nothing, on exactly the
    sources whose descriptors ask for both concurrency and politeness.
    """
    inner = RecordingTransport()
    paced = PacedTransport(inner, min_interval_seconds=0.05, per_host_concurrency=4)

    async def scenario():
        await asyncio.gather(
            *[paced.send(HttpRequest(url=f"https://one.example/{i}")) for i in range(4)]
        )

    asyncio.run(scenario())

    assert len(inner.started_at) == 4
    times = sorted(inner.started_at)
    # Asserted on the SPAN rather than on each gap: a per-gap threshold a hair under
    # the interval is a 5ms differential away from flaking, while the span over four
    # requests is ~0.15s paced and ~0s unpaced — a difference no scheduling jitter
    # can close.
    span = times[-1] - times[0]
    assert span >= 0.12, f"concurrent requests were not paced: {times}"
    assert all(b - a >= 0.04 for a, b in zip(times, times[1:])), f"uneven pacing: {times}"


def test_a_request_is_paced_from_when_the_previous_one_started():
    """The slot is claimed at the start of a request, not stamped at its end, so the
    interval is the gap between requests arriving at the host rather than a gap
    appended to however long the host took to answer."""

    class SlowTransport(RecordingTransport):
        delay = 0.08

        async def send(self, request):
            await asyncio.sleep(self.delay)
            return await super().send(request)

    inner = SlowTransport()
    paced = PacedTransport(inner, min_interval_seconds=0.05, per_host_concurrency=1)
    inner.delay = 0.08

    async def scenario():
        loop = asyncio.get_running_loop()
        started = loop.time()
        for i in range(3):
            await paced.send(HttpRequest(url=f"https://one.example/{i}"))
        return loop.time() - started

    elapsed = asyncio.run(scenario())
    # Three 0.08s requests dominate; each 0.05s interval is already spent by the time
    # its request returns, so pacing must add nothing (~0.24s). Stamping at
    # completion instead would append an interval to each of the last two (~0.34s).
    # The threshold sits midway, ~50ms clear of either outcome.
    assert elapsed < 0.29, f"pacing was appended to the response time: {elapsed:.3f}s"
