"""JobSpy aggregators (Indeed / LinkedIn / …) — the one `SUBPROCESS` adapter.

Supersedes `scraper.src_jobspy`. Every other source is an HTTP endpoint this
process can call; JobSpy is a *library* (`jobspy.scrape_jobs`) that blocks the
calling thread for minutes inside pandas and its own HTTP stack. Running that on
the scheduler's event loop would freeze every other source for the duration and
would be uncancellable at the deadline, so the work is exiled to a child process
(`ExecutionMode.SUBPROCESS`) that streams `NormalizedPosting.to_json_dict()`
NDJSON back over stdout. The parent re-hydrates each line with `from_json_dict`
and yields it — which is exactly why the record type is required to be JSON
round-trippable.

From the outside this is an ordinary async adapter: `plan()` is pure, `fetch()`
is an async generator of records, failures are classified `SourceError`s, and
the scheduler needs no special case. `ExecutionMode.SUBPROCESS` is a *declaration
of isolation*, not a different calling convention: it tells the scheduler that
this source's work is already fenced off in its own process (so a hung scrape
cannot wedge the loop, and cancellation genuinely kills the work) and that its
deadline is minutes rather than seconds.

What shapes this adapter, beyond the subprocess:

  * ONE TARGET PER SITE. `config.jobspy.sites` is `["indeed", "linkedin"]`, so
    `plan()` emits one target per site with `instance_key=site`. Failure
    isolation is the point: LinkedIn rate-limiting must not fail Indeed, and
    each site gets its own `source_runs` row, its own attempt count, and its own
    child process. It also gives each site its own identity namespace
    (`jobspy:indeed`).
  * PARTIAL, ALWAYS. Every query is a keyword search over a location. Not
    seeing a posting proves only that the query did not match it, so absence
    must never be inferred (contract: `InventoryScope`).
  * NO `req_id`. An aggregator row is a mirror of somebody else's requisition;
    the numbers Indeed and LinkedIn mint are theirs, not the employer's. The
    record therefore carries a URL claim only, and Phase 3 resolves those
    aggregator URLs against direct-source inventory. Fabricating a `req_id`
    here would mint a permanent identity for a mirror.
  * IN-RUN DEDUPE AND A REPOST CAP. Aggregator queries overlap ~40% and
    staffing agencies repost one job dozens of times. `RepostFilter` reproduces
    `scraper.src_jobspy`'s cleanup pass as a pure stream transform. It is an
    efficiency measure only (contract invariant 5) — dropping a record here can
    never be load-bearing for correctness. Cross-*source* merging (the old
    `scraper.dedupe`) is Phase 3 resolver work and is deliberately absent.
  * CHECKPOINTED PER QUERY. A site's work is the `search_terms x searches`
    cross product, several minutes of it. The cursor is `{"query_index": n}`:
    every query before `n` has been fully yielded. A resumed run tells the
    child to skip them.

Deviations from `scraper.src_jobspy`, all of them required by the contract:

  * a failing query raises instead of being printed and skipped (invariant 3:
    a blocked site must not be indistinguishable from an empty one). The
    checkpoint means the retry resumes at the failed query rather than redoing
    the run;
  * `jobspy` not being installed raises `PermanentSourceError` instead of
    returning `[]`;
  * pandas `NaN` is treated as missing everywhere, including `is_remote`, where
    the legacy `bool(r_.get("is_remote"))` silently reported every NaN row as
    remote (`bool(float("nan"))` is `True`);
  * salary bounds are formatted canonically (`120000-160000 yearly`), because
    pandas hands back `120000` or `120000.0` for the same posting depending on
    what else is in the column, and `salary_text` is hashed;
  * URL dedupe keys on `normalize_url` rather than the raw string, so a
    tracking parameter cannot smuggle a duplicate past the filter.

The legacy per-row remote flag is preserved exactly, bug note and all: a remote
*search* also returns on-site roles, so `search.is_remote` must NOT force
`remote=True` on the row.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import deque
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..contract import (
    ConfigError,
    Disposition,
    ExecutionMode,
    FetchContext,
    InventoryScope,
    JSONValue,
    NormalizedPosting,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceCategory,
    SourceConfig,
    SourceDescriptor,
    SourceTarget,
    TransientSourceError,
    TransportKind,
    normalize_date,
)

__all__ = [
    "CHILD_MODULE",
    "DESCRIPTOR",
    "ChildMessage",
    "JobSpyAdapter",
    "RepostFilter",
    "build_queries",
    "child_command",
    "dedupe_stream",
    "decode_line",
    "encode_error_line",
    "encode_progress_line",
    "encode_record_line",
    "google_search_term",
    "normalize_row",
    "parse_rows",
    "salary_text",
    "task_spec",
]

SOURCE_KEY = "jobspy"
#: Free-standing `jobspy` block in `config.json` (not under `companies`), which
#: `SourceConfig.from_mapping` files under `options`.
CONFIG_KEY = "jobspy"

#: `python -m` target for the child. Its package root is `webapp/`, which is
#: also what `[tool.pytest.ini_options].pythonpath` puts on the path.
CHILD_MODULE = "backend.sources.adapters.jobspy_child"
PACKAGE_ROOT = Path(__file__).resolve().parents[3]
REPO_ROOT = PACKAGE_ROOT.parent

#: Wire protocol version. Bumped only when the NDJSON line shapes change; the
#: child refuses a spec it does not understand rather than guessing.
WIRE_VERSION = 1
LINE_RECORD = "record"
LINE_PROGRESS = "progress"
LINE_ERROR = "error"

#: Exit codes the child uses to pre-classify its own failure. Anything else
#: nonzero (a crash, an OOM kill, a signal) is treated as transient: the child
#: is a whole process and dying is more often environmental than structural.
EXIT_OK = 0
EXIT_TRANSIENT = 75
EXIT_PERMANENT = 78

#: Legacy `_desc[:6000]`: descriptions are captured at scrape time so the
#: description pass need not re-scrape Indeed (results drift between passes).
DESCRIPTION_LIMIT = 6000
#: `config.jobspy.title_cap`, default 5 in `scraper.src_jobspy`.
DEFAULT_TITLE_CAP = 5
DEFAULT_COUNTRY = "USA"
DEFAULT_RESULTS_WANTED = 100
#: `scraper.src_jobspy`'s fallback when `config.jobspy.searches` is absent.
DEFAULT_SEARCHES: tuple[Mapping[str, JSONValue], ...] = (
    {"location": "San Francisco Bay Area, CA", "is_remote": False},
)

#: A record line carries an inline description, so 64KiB (asyncio's default
#: stream limit) is uncomfortably close. 1MiB leaves headroom without letting a
#: runaway child buffer without bound.
STREAM_LIMIT = 1024 * 1024
#: How long SIGTERM gets before SIGKILL when shutting the child down.
TERMINATE_GRACE_SECONDS = 5.0
#: Kept for the error message only; the child's stderr also carries jobspy's own
#: chatter, which is why it is drained continuously (a full pipe would otherwise
#: block the child mid-scrape).
STDERR_TAIL_LINES = 20

DESCRIPTOR = SourceDescriptor(
    source_key=SOURCE_KEY,
    category=SourceCategory.AGGREGATOR,
    # Aggregators are their own run kind by roadmap decision: an aggregator
    # outage must never degrade or delay the direct-source daily run.
    run_kinds=frozenset({RunKind.AGGREGATORS}),
    refresh_interval_seconds=12 * 3600,
    # A site's whole `terms x searches` cross product runs inside one child.
    # Minutes, not seconds — and the isolation is what makes that affordable.
    default_deadline_seconds=600.0,
    # Per-query resume: a run cut off at the deadline must not restart the
    # queries whose records were already delivered.
    supports_checkpoint=True,
    execution=ExecutionMode.SUBPROCESS,
    # The child owns its own network stack (jobspy's). The parent is handed no
    # transport, and `ctx.http()` correctly raises if this adapter ever asks.
    transport=TransportKind.NONE,
    # Two sites means two heavy children; more would be a scrape the host
    # notices. Not a politeness knob — jobspy paces its own requests.
    max_concurrent_targets=2,
    per_host_concurrency=1,
    min_request_interval_seconds=0.0,
    # jobspy returns the description with the row, so Phase 3.2 can skip the
    # description fetch entirely for these records.
    description_inline=True,
    default_inventory_scope=InventoryScope.PARTIAL,
)


# --------------------------------------------------------------------------- #
# Row normalization (pure)
# --------------------------------------------------------------------------- #
def _missing(value: Any) -> bool:
    """`None`, pandas `NaN`, or pandas `NaT`.

    `scraper.src_jobspy`'s `s()` helper existed for exactly this: a DataFrame
    cell is `NaN` rather than `None` whenever any row in the column is unset.
    `NaN != NaN` catches both `NaN` and `NaT`.
    """
    if value is None:
        return True
    return isinstance(value, float) and value != value


def _text(value: Any) -> str:
    return "" if _missing(value) else str(value)


def _truthy(value: Any) -> bool:
    """Boolean coercion that does not treat `NaN` as `True`.

    `bool(float("nan"))` is `True`, so the legacy `bool(r_.get("is_remote"))`
    marked every row with an unset remote flag as remote. That polluted the
    remote bucket in the opposite direction from the bug the legacy comment
    warns about, and is fixed here.
    """
    if _missing(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "t")
    return bool(value)


def _amount(value: Any) -> str:
    """Canonical spelling of a salary bound.

    pandas hands back `120000` or `120000.0` for the same posting depending on
    whether anything else in the column is `NaN`. `salary_text` is hashed, so an
    unstable spelling would mint a spurious posting version on alternate runs.
    """
    if _missing(value) or isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    return str(value).strip()


def salary_text(row: Mapping[str, Any]) -> str:
    """`min_amount`/`max_amount`/`interval` -> `"120000-160000 yearly"`.

    Follows `scraper.src_jobspy`: no `min_amount` means no salary at all, even
    when a maximum is present, because a lone upper bound is not a range any
    reader can use. Equal bounds collapse to a single figure, and a missing
    upper bound is omitted rather than rendered as the literal `"None"` the
    legacy f-string produced.
    """
    low = _amount(row.get("min_amount"))
    if not low:
        return ""
    high = _amount(row.get("max_amount"))
    interval = _text(row.get("interval")).strip()
    amount = f"{low}-{high}" if high and high != low else low
    return f"{amount} {interval}".strip()


def normalize_row(
    row: Mapping[str, Any],
    target: SourceTarget,
    *,
    query: Mapping[str, Any] | None = None,
) -> NormalizedPosting | None:
    """One jobspy DataFrame row -> a record, or `None` if it is unusable.

    Pure: no I/O, no clock, no globals. Returns `None` rather than raising for a
    row with no title or no `job_url` — such a row cannot be identified or
    opened, and one bad row must not fail a query (contract: a bad *item* is
    skipped, a bad *envelope* raises).

    `req_id` is deliberately `None`. See the module docstring: an aggregator row
    is a mirror, its identity evidence is the URL alone, and Phase 3 resolves
    that against direct inventory.
    """
    title = _text(row.get("title")).strip()
    url = _text(row.get("job_url")).strip()
    if not title or not url:
        return None

    posted_raw = _text(row.get("date_posted")).strip()
    description = _text(row.get("description"))
    extra: dict[str, JSONValue] = {"site": target.instance_key or _text(row.get("site"))}
    job_id = _text(row.get("id")).strip()
    if job_id:
        # Provenance only. Indeed's `in-4f1c2` is Indeed's handle on somebody
        # else's requisition, so it must not become this record's identity.
        extra["aggregator_job_id"] = job_id
    if query:
        extra["search_term"] = _text(query.get("term"))
        extra["search_location"] = _text(query.get("location"))
        extra["search_is_remote"] = bool(query.get("is_remote"))

    return target.record(
        title=title,
        company=_text(row.get("company")),
        url=url,
        location=_text(row.get("location")),
        req_id=None,
        posted_date=normalize_date(posted_raw),
        posted_raw=posted_raw,
        salary_text=salary_text(row),
        # Trust jobspy's per-row remote flag. A remote SEARCH also returns
        # on-site roles, so the search's `is_remote` must NOT force this to
        # True — that polluted the remote bucket (legacy comment, preserved).
        remote=_truthy(row.get("is_remote")),
        description=description[:DESCRIPTION_LIMIT] if description.strip() else None,
        extra=extra,
    )


def parse_rows(
    payload: bytes | str | Sequence[Mapping[str, Any]],
    target: SourceTarget,
    *,
    query: Mapping[str, Any] | None = None,
) -> Iterator[NormalizedPosting]:
    """A query's rows -> records. Pure, lazy, and the fixture entry point.

    Accepts the JSON array a frozen fixture stores, or the already-parsed list
    of dicts `DataFrame.to_dict(orient="records")` produces in the child. A
    payload that is not a list of objects raises `PayloadError`: that is a
    broken envelope, meaning jobspy changed shape and this adapter is wrong.
    """
    if isinstance(payload, (bytes, bytearray, str)):
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"jobspy {target.instance_key}: rows payload is not JSON: {exc}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
    else:
        data = payload

    if not isinstance(data, Sequence) or isinstance(data, (str, bytes, bytearray)):
        raise PayloadError(
            f"jobspy {target.instance_key}: expected a list of rows, got {type(data).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )
    for row in data:
        if not isinstance(row, Mapping):
            continue
        record = normalize_row(row, target, query=query)
        if record is not None:
            yield record


# --------------------------------------------------------------------------- #
# In-run dedupe and repost cap (pure stream transform)
# --------------------------------------------------------------------------- #
class RepostFilter:
    """`scraper.src_jobspy`'s cleanup pass, as a streaming predicate.

    Two suppressions, both reproduced from the legacy pass and both EFFICIENCY
    ONLY (contract invariant 5 — the writer dedupes on identity, so nothing here
    is load-bearing for correctness):

      * first-seen URL wins. Aggregator queries overlap heavily (~40% duplicate
        URLs across terms), and re-emitting is merely wasteful. The key is
        `normalize_url`, not the raw string, so `?utm_source=…` cannot smuggle a
        duplicate through.
      * at most `title_cap` rows per `(company, title)`. Staffing agencies post
        one job dozens of times under distinct URLs; without the cap a single
        agency crowds out a whole run's worth of real postings. The counter is
        incremented for every non-duplicate row, including the ones it then
        rejects, exactly as the legacy pass did.

    Stateful for the length of one `fetch` call (one target). Cross-target and
    cross-source merging is Phase 3 resolver work, not this.
    """

    __slots__ = ("_seen_urls", "_title_counts", "title_cap")

    def __init__(self, *, title_cap: int = DEFAULT_TITLE_CAP) -> None:
        #: `<= 0` disables the cap rather than rejecting everything: a
        #: configured zero has never meant "emit nothing".
        self.title_cap = int(title_cap)
        self._seen_urls: set[str] = set()
        self._title_counts: dict[tuple[str, str], int] = {}

    def accept(self, record: NormalizedPosting) -> bool:
        key_url = record.url_key or record.url
        if not key_url or key_url in self._seen_urls:
            return False
        self._seen_urls.add(key_url)
        key = (record.company.lower(), record.title.lower())
        count = self._title_counts.get(key, 0) + 1
        self._title_counts[key] = count
        return self.title_cap <= 0 or count <= self.title_cap


def dedupe_stream(
    records: Iterable[NormalizedPosting], *, title_cap: int = DEFAULT_TITLE_CAP
) -> Iterator[NormalizedPosting]:
    """`RepostFilter` over an iterable. Lazy, so `fetch` keeps streaming."""
    keep = RepostFilter(title_cap=title_cap)
    for record in records:
        if keep.accept(record):
            yield record


# --------------------------------------------------------------------------- #
# Query planning (pure)
# --------------------------------------------------------------------------- #
def google_search_term(term: str, *, is_remote: bool) -> str:
    """The Google-flavoured phrasing `scraper.src_jobspy` built, unchanged.

    Only Google Jobs consumes it; the other sites ignore the argument. Kept
    identical so a site swap does not silently change what is searched.
    """
    where = "remote in the US" if is_remote else "in the San Francisco Bay Area"
    return f"{term} jobs {where}"


def _searches(raw: Any) -> tuple[Mapping[str, JSONValue], ...]:
    """`config.jobspy.searches` -> `({"location", "is_remote"}, …)`."""
    entries: list[Mapping[str, JSONValue]] = []
    for entry in raw or ():
        if not isinstance(entry, Mapping):
            continue
        location = str(entry.get("location") or "").strip()
        if not location:
            continue
        entries.append({"location": location, "is_remote": bool(entry.get("is_remote"))})
    return tuple(entries) or DEFAULT_SEARCHES


def build_queries(target: SourceTarget) -> tuple[Mapping[str, JSONValue], ...]:
    """The `search_terms x searches` cross product this target must run.

    Pure and DETERMINISTICALLY ORDERED, because the checkpoint cursor is an
    index into this tuple. Term-outer/search-inner matches
    `scraper.src_jobspy`'s loop nesting (the site loop is the target here).
    Changing the terms or the searches changes `config_fingerprint`, so the
    scheduler discards a stale checkpoint rather than resuming into a different
    result set (contract: `Checkpoint.is_valid_for`).
    """
    terms = tuple(str(t).strip() for t in (target.param("search_terms") or ()) if str(t).strip())
    searches = _searches(target.param("searches"))
    wanted = int(target.param("results_wanted") or DEFAULT_RESULTS_WANTED)
    queries: list[Mapping[str, JSONValue]] = []
    for term in terms:
        for search in searches:
            is_remote = bool(search.get("is_remote"))
            queries.append(
                {
                    "term": term,
                    "location": str(search.get("location") or ""),
                    "is_remote": is_remote,
                    "google_search_term": google_search_term(term, is_remote=is_remote),
                    "results_wanted": wanted,
                }
            )
    return tuple(queries)


def task_spec(target: SourceTarget, *, start_index: int = 0) -> dict[str, JSONValue]:
    """The JSON the parent writes to the child's stdin. Pure and JSON-safe."""
    return {
        "version": WIRE_VERSION,
        "source_key": SOURCE_KEY,
        "instance_key": target.instance_key,
        "site": str(target.param("site") or target.instance_key),
        "country_indeed": str(target.param("country_indeed") or DEFAULT_COUNTRY),
        "hours_old": target.param("hours_old"),
        "start_index": int(start_index),
        "queries": [dict(query) for query in build_queries(target)],
    }


# --------------------------------------------------------------------------- #
# NDJSON wire protocol (pure)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ChildMessage:
    """One decoded stdout line. Exactly one of the payload fields is set."""

    kind: str
    record: NormalizedPosting | None = None
    query_index: int = -1
    count: int = 0
    disposition: str = ""
    message: str = ""


def encode_record_line(record: NormalizedPosting) -> str:
    return json.dumps(
        {"type": LINE_RECORD, "record": record.to_json_dict()}, ensure_ascii=False
    ) + "\n"


def encode_progress_line(query_index: int, *, count: int = 0) -> str:
    """Emitted after every record of query `query_index` has been written."""
    return json.dumps(
        {"type": LINE_PROGRESS, "query_index": int(query_index), "count": int(count)}
    ) + "\n"


def encode_error_line(disposition: str, message: str) -> str:
    return json.dumps({"type": LINE_ERROR, "disposition": disposition, "message": message}) + "\n"


def decode_line(line: bytes | str, target: SourceTarget) -> ChildMessage:
    """One stdout line -> `ChildMessage`, or `PayloadError`.

    Strict on purpose. The child redirects jobspy's own chatter to stderr, so
    stdout is protocol and nothing else; a line that does not parse means the
    contract between parent and child broke, which no retry fixes. A record
    whose namespace does not match the target is equally fatal — accepting it
    would file a posting under an identity namespace that was never enumerated.
    """
    text = line.decode("utf-8", errors="replace") if isinstance(line, (bytes, bytearray)) else line
    text = text.strip()
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise PayloadError(
            f"jobspy {target.instance_key}: child emitted a non-JSON line ({exc}): {text[:120]!r}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        ) from exc
    if not isinstance(payload, Mapping):
        raise PayloadError(
            f"jobspy {target.instance_key}: child line is not an object, got {type(payload).__name__}",
            source_key=SOURCE_KEY,
            instance_key=target.instance_key,
        )

    kind = payload.get("type")
    if kind == LINE_RECORD:
        raw = payload.get("record")
        if not isinstance(raw, Mapping):
            raise PayloadError(
                f"jobspy {target.instance_key}: record line carries no record object",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            )
        record = NormalizedPosting.from_json_dict(raw)
        if record.source_key != target.source_key or record.instance_key != target.instance_key:
            raise PayloadError(
                f"jobspy {target.instance_key}: child emitted a record for namespace "
                f"{record.namespace!r}",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            )
        return ChildMessage(kind=LINE_RECORD, record=record)
    if kind == LINE_PROGRESS:
        try:
            index = int(payload.get("query_index"))
        except (TypeError, ValueError) as exc:
            raise PayloadError(
                f"jobspy {target.instance_key}: progress line has no usable query_index",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            ) from exc
        return ChildMessage(
            kind=LINE_PROGRESS, query_index=index, count=int(payload.get("count") or 0)
        )
    if kind == LINE_ERROR:
        return ChildMessage(
            kind=LINE_ERROR,
            disposition=str(payload.get("disposition") or Disposition.TRANSIENT),
            message=str(payload.get("message") or "child reported an unspecified failure"),
        )
    raise PayloadError(
        f"jobspy {target.instance_key}: unknown child line type {kind!r}",
        source_key=SOURCE_KEY,
        instance_key=target.instance_key,
    )


# --------------------------------------------------------------------------- #
# Child process plumbing
# --------------------------------------------------------------------------- #
def child_command(target: SourceTarget) -> list[str]:
    """The argv that starts the child.

    `params["child_command"]` overrides it. That exists so tests can drive
    `fetch()` with a fake child (no jobspy, no network) and so an operator can
    pin a different interpreter; it comes from `plan()`/config, which is the
    same trust boundary as every other param.
    """
    override = target.param("child_command")
    if override:
        return [str(part) for part in override]
    return [sys.executable, "-m", CHILD_MODULE]


def child_env() -> dict[str, str]:
    """Environment for the child: `webapp/` on `PYTHONPATH`, unbuffered stdio.

    Unbuffered matters for the streaming guarantee (contract invariant 6) — a
    block-buffered child would hold minutes of records before the parent saw
    the first one.
    """
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(PACKAGE_ROOT) + (os.pathsep + existing if existing else "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


async def _drain_stderr(stream: asyncio.StreamReader | None, into: deque[str]) -> None:
    """Continuously consume the child's stderr, keeping only the tail.

    Not optional: jobspy logs to stderr, and an unread pipe fills and blocks the
    child mid-scrape. The tail is used only to make a failure legible.
    """
    if stream is None:
        return
    while True:
        chunk = await stream.readline()
        if not chunk:
            return
        text = chunk.decode("utf-8", errors="replace").rstrip()
        if text:
            into.append(text)


async def _stop_child(proc: asyncio.subprocess.Process) -> None:
    """SIGTERM, then SIGKILL, then reap. Idempotent, and never leaves an orphan.

    Called from both the cancellation path and the `finally`, so a deadline
    cancellation, a `PayloadError`, and a clean finish all converge on a dead,
    reaped child. `asyncio.shield` keeps the reaping task alive even when the
    surrounding await is itself cancelled, which is precisely the case this has
    to survive.
    """
    if proc.returncode is not None:
        return
    with suppress(ProcessLookupError):
        proc.terminate()
    waiter = asyncio.ensure_future(proc.wait())
    try:
        await asyncio.wait_for(asyncio.shield(waiter), TERMINATE_GRACE_SECONDS)
        return
    except (TimeoutError, asyncio.CancelledError):
        pass
    with suppress(ProcessLookupError):
        proc.kill()
    with suppress(asyncio.CancelledError):
        await asyncio.shield(waiter)


def _failure(
    target: SourceTarget, message: str, *, disposition: str, tail: Iterable[str]
) -> Exception:
    # `tail` is a deque, which does not slice: materialize the last few lines.
    detail = " | ".join(list(tail)[-3:])
    text = f"jobspy {target.instance_key}: {message}"
    if detail:
        text = f"{text} (stderr: {detail})"
    cls = (
        TransientSourceError
        if str(disposition) == str(Disposition.TRANSIENT)
        else PermanentSourceError
    )
    return cls(text, source_key=SOURCE_KEY, instance_key=target.instance_key)


class JobSpyAdapter:
    """Stateless singleton. See `SourceAdapter`."""

    descriptor = DESCRIPTOR

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]:
        """`config.jobspy` + `profile.search_terms` -> one target per site.

        Pure. No sites or no search terms plans zero targets, which is not an
        error — the scheduler simply has no aggregator work.
        """
        block = config.option(CONFIG_KEY) or {}
        if not isinstance(block, Mapping):
            raise ConfigError(
                f"jobspy config must be an object, got {type(block).__name__}",
                source_key=SOURCE_KEY,
            )
        terms = tuple(str(t).strip() for t in config.search_terms if str(t).strip())
        if not terms:
            return []
        sites = tuple(str(s).strip() for s in (block.get("sites") or ()) if str(s).strip())
        if not sites:
            return []

        searches = _searches(block.get("searches"))
        default_wanted = int(block.get("results_wanted_per_site") or DEFAULT_RESULTS_WANTED)
        country = str(block.get("country_indeed") or DEFAULT_COUNTRY)
        hours_old = block.get("hours_old")
        title_cap = block.get("title_cap")
        title_cap = DEFAULT_TITLE_CAP if title_cap is None else int(title_cap)

        targets: list[SourceTarget] = []
        for site in sites:
            wanted = block.get(f"results_wanted_{site}")
            targets.append(
                SourceTarget(
                    source_key=SOURCE_KEY,
                    # Namespaces identity per site (`jobspy:indeed`) and gives
                    # each site its own run row, deadline, and child process.
                    instance_key=site,
                    label=f"JobSpy {site}",
                    params={
                        "site": site,
                        "search_terms": terms,
                        "searches": searches,
                        "results_wanted": int(wanted) if wanted is not None else default_wanted,
                        "country_indeed": country,
                        "hours_old": hours_old,
                        "title_cap": title_cap,
                    },
                    inventory_scope=DESCRIPTOR.default_inventory_scope,
                    # No HTTP transport is handed to this adapter; the child
                    # owns its own requests, so there is no host to limit on.
                    host=None,
                )
            )
        return targets

    async def fetch(
        self, target: SourceTarget, ctx: FetchContext
    ) -> AsyncIterator[NormalizedPosting]:
        """Run one site's queries in a child process, streaming its NDJSON.

        Records are yielded as their lines arrive (invariant 6), a checkpoint is
        marked at each query boundary — after that query's records have been
        delivered, which an async generator's suspension proves — and the child
        is terminated on every exit path, including cancellation. No retry, no
        sleep, no deadline branching: everything this method knows how to do on
        failure is raise (invariants 1-3, 8).
        """
        queries = build_queries(target)
        if not queries:
            raise ConfigError(
                "jobspy: target has no search terms to run",
                source_key=SOURCE_KEY,
                instance_key=target.instance_key,
            )

        start_index = 0
        emitted = 0
        if ctx.resume_from is not None:
            start_index = max(0, int(ctx.resume_from.cursor.get("query_index") or 0))
            emitted = max(0, int(ctx.resume_from.emitted or 0))
        if start_index >= len(queries):
            # Everything this target had to do was already delivered. Spawning a
            # child to discover that would cost minutes for nothing.
            return

        spec = task_spec(target, start_index=start_index)
        proc = await asyncio.create_subprocess_exec(
            *child_command(target),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(REPO_ROOT),
            env=child_env(),
            limit=STREAM_LIMIT,
        )
        tail: deque[str] = deque(maxlen=STDERR_TAIL_LINES)
        stderr_task = asyncio.ensure_future(_drain_stderr(proc.stderr, tail))
        keep = RepostFilter(title_cap=int(target.param("title_cap") or DEFAULT_TITLE_CAP))
        reported: ChildMessage | None = None
        returncode = 0

        try:
            if proc.stdin is not None:
                try:
                    proc.stdin.write(json.dumps(spec).encode("utf-8"))
                    await proc.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    # The child died before reading its spec; its exit code and
                    # stderr tail below are what classify that, not this.
                    pass
                finally:
                    with suppress(OSError):
                        proc.stdin.close()

            assert proc.stdout is not None  # PIPE was requested above
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                if not line.strip():
                    continue
                message = decode_line(line, target)
                if message.kind == LINE_RECORD and message.record is not None:
                    if keep.accept(message.record):
                        emitted += 1
                        yield message.record
                elif message.kind == LINE_PROGRESS:
                    # Reaching here proves the consumer took delivery of every
                    # record of this query (contract: DELIVERED, NOT COMMITTED).
                    ctx.mark_checkpoint(
                        {"query_index": message.query_index + 1}, target=target, emitted=emitted
                    )
                elif message.kind == LINE_ERROR:
                    reported = message
                    break
            returncode = await proc.wait()
        except asyncio.CancelledError:
            await _stop_child(proc)
            raise
        finally:
            await _stop_child(proc)
            stderr_task.cancel()
            with suppress(asyncio.CancelledError):
                await stderr_task

        if reported is not None:
            raise _failure(target, reported.message, disposition=reported.disposition, tail=tail)
        if returncode != 0:
            disposition = (
                Disposition.PERMANENT if returncode == EXIT_PERMANENT else Disposition.TRANSIENT
            )
            raise _failure(
                target,
                f"child exited with code {returncode}",
                disposition=str(disposition),
                tail=tail,
            )


ADAPTER = JobSpyAdapter()
