"""Phase 3.3's scoring graph: what to score, in what order, and how much.

The roadmap lines this implements: "resolve -> enrich -> score as a graph over
changed inputs", "score exactly once per (posting version, profile version,
scorer)", and "never scan all postings after each source".

THE SHAPE

    direct-inventory index  ->  resolve  ->  enrichment  ->  score
                                (bridge +      (protocol
                                 aggregator      slot)
                                 matching)

Each stage is a `writer.WriteOp` (`events` plus `apply(conn) -> object | None`),
which is the entire Phase 4 wiring story: a scheduler run submits them through
`writer.submit` in this order and neither `scheduler.py` nor `writer.py` changes
by a line. `run_pass` below applies them in sequence against a caller-owned
connection, which is what the tests and the benchmark drive.

THE ENRICHMENT SLOT IS A PROTOCOL, NOT AN IMPORT. Phase 3.2 owns description
fetching, and it is being built in parallel with this. The graph declares the
shape it will call (`EnrichmentStage`) and defaults to `null_enrichment`, which
does nothing and returns nothing. That is a real default, not a placeholder: a
posting with no description on file scores with `description_identity=""`, which
is a scored FACT (rule zero caps its tier) rather than a missing input -- and when
the description later arrives, its different `input_hash` supersedes that score.

TWO PASS MODES, AND WHY THE BASELINE IS LICENSED RATHER THAN ASSUMED

  FULL          no completed prior pass, or the profile or the scorer moved. The
                work list is the whole corpus, cursor-paginated by posting id.
  INCREMENTAL   the run's dirty set (`runstore.dirty_posting_ids`) UNION the open
                `score_invalidations`. Everything else -- the overwhelming
                majority of a daily run -- is not looked at.

The baseline is licensed ONLY by a COMPLETED pass on the run
`dirty_posting_ids` measures against. That is the same self-healing rule
`runstore._CONSUMED_RUN_STATUSES` applies one layer down, and it exists for the
same reason: a pass that died after its run succeeded has already let the next
run's dirty set be measured against work that was never actually done, so
trusting any prior pass would silently drop it. A pass that dies re-does its work.

WHAT MAKES A CANONICAL MATCH WORTH ANYTHING. A posting's content state is a map
`{namespace: posting_version_id}` (Phase 3.1), one entry per observing source. The
canonical version is picked from that map by SOURCE CATEGORY rank -- direct and
startup boards over manual over aggregators -- and after a relink the SURVIVOR's
selection also considers the state maps of postings redirecting into it. That is
the whole point of resolving: the survivor's canonical version becomes the board's
dated, described record instead of the aggregator's undated one, so the
`undated-aggregator` and `needs-desc` caps stop firing and the tier actually
MOVES. A resolution that only rewrote bookkeeping would rescore to the same
number.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable

from . import registry, resolver, runstore, scoring
from .contract import SourceCategory
from .writer import RunEvent

__all__ = [
    "CATEGORY_RANK",
    "ClosePass",
    "EnrichmentOutcome",
    "EnrichmentStage",
    "OpenPass",
    "PASS_COMPLETED",
    "PASS_FAILED",
    "PASS_RUNNING",
    "PassContext",
    "PassDecision",
    "PassIdentity",
    "PassMode",
    "PassRecord",
    "ResolvePass",
    "ScoreGraphPass",
    "baseline_pass",
    "baseline_run_uid",
    "build_work_rows",
    "decide_mode",
    "null_enrichment",
    "registry_category",
    "run_pass",
    "select_canonical_version",
    "select_work",
]

PASS_RUNNING = "running"
PASS_COMPLETED = "completed"
PASS_FAILED = "failed"

#: How many ids one prefetch statement carries. Mirrors `runstore._LOOKUP_CHUNK`.
_LOOKUP_CHUNK = 400

#: Default work-list page. Big enough that a daily run's dirty set is one or two
#: pages, small enough that a FULL pass over 33,500 postings holds a few hundred
#: rows in memory rather than the corpus.
DEFAULT_BATCH_SIZE = 500

_PASS_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://jobhunt.local/canonical/score-pass")


class PassMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass(frozen=True, slots=True)
class PassIdentity:
    """What a pass scores WITH: the candidate data and the scoring code."""

    profile_version_id: str
    scorer_hash: str


@dataclass(frozen=True, slots=True)
class PassRecord:
    """A `score_passes` row, as `decide_mode` needs to see it."""

    pass_id: str
    run_uid: str
    profile_version_id: str
    scorer_hash: str
    mode: str
    status: str


@dataclass(frozen=True, slots=True)
class PassDecision:
    mode: PassMode
    #: Why, in declared order. Empty for INCREMENTAL: nothing forced a full pass.
    reasons: tuple[str, ...] = ()


def decide_mode(current: PassIdentity, baseline: PassRecord | None) -> PassDecision:
    """FULL or INCREMENTAL, and why. PURE -- no connection, no clock.

    Three rules, and each one exists because breaking it leaves scores that are
    silently wrong rather than merely stale:

      no baseline pass   nothing has ever completed a pass against the run this
                         run's changes are measured from, so there is no set of
                         "already scored" postings to trust. Includes the very
                         first pass, and every pass after one that died.
      profile changed    a different `profile_version_id` means every stored tier
                         was produced against different candidate preferences. An
                         incremental pass would leave the untouched majority of
                         the corpus scored against a profile that no longer
                         exists.
      scorer changed     likewise for the code. `scorer_hash` mixes
                         RUBRIC_VERSION with the pinned source digest, so this
                         fires on an edit whether or not anyone bumped the string.

    Both identity reasons are reported when both apply: "why did tonight's run
    rescore 33,000 postings" has to be answerable from the pass row.
    """
    if baseline is None:
        return PassDecision(mode=PassMode.FULL, reasons=("no_baseline_pass",))
    reasons = []
    if baseline.profile_version_id != current.profile_version_id:
        reasons.append("profile_changed")
    if baseline.scorer_hash != current.scorer_hash:
        reasons.append("scorer_changed")
    if reasons:
        return PassDecision(mode=PassMode.FULL, reasons=tuple(reasons))
    return PassDecision(mode=PassMode.INCREMENTAL)


# --------------------------------------------------------------------------- #
# Canonical version selection (pure, injectable rank)
# --------------------------------------------------------------------------- #
#: Rank of a source category when choosing which source's version is canonical.
#: DIRECT and STARTUP_BOARD tie at the top: both are first-party listings with
#: their own requisition identity, dated and usually described. MANUAL is a human
#: assertion, so it outranks a scrape of somebody else's board. AGGREGATOR is
#: last, which is the entire mechanism behind the ghost-listing caps: an
#: aggregator's undated mirror is only canonical when nothing better exists.
CATEGORY_RANK: Mapping[SourceCategory, int] = {
    SourceCategory.DIRECT: 3,
    SourceCategory.STARTUP_BOARD: 3,
    SourceCategory.MANUAL: 2,
    SourceCategory.AGGREGATOR: 1,
}


def registry_category(namespace: str) -> SourceCategory:
    """A namespace's source category, from the adapter registry.

    `"greenhouse:acme"` -> the Greenhouse descriptor's category. An UNREGISTERED
    source key reads as AGGREGATOR, which is the conservative direction twice
    over: it cannot outrank a real direct board in canonical selection, and it can
    never be treated as direct inventory the resolver merges aggregators INTO. A
    fake or retired source key therefore degrades to "trust it least" rather than
    to an exception that takes out the pass.
    """
    source_key = namespace.split(":", 1)[0] if namespace else ""
    if source_key and registry.is_registered(source_key):
        return registry.get(source_key).descriptor.category
    return SourceCategory.AGGREGATOR


def select_canonical_version(
    state_maps: Sequence[Mapping[str, str]],
    *,
    category_of: Callable[[str], SourceCategory] = registry_category,
) -> tuple[str, str] | None:
    """(namespace, posting_version_id) for the version that speaks for a posting.

    PURE, and the rank is injectable so the decision can be exercised without a
    registry. `state_maps` is given in PRIORITY ORDER -- the posting's own map
    first, then the maps of postings redirecting into it -- and the first map to
    mention a namespace wins for that namespace, so a merge cannot let a loser's
    stale entry override the survivor's live one for the same source.

    Selection is by category rank, ties broken LEXICOGRAPHICALLY by namespace.
    The tiebreak is arbitrary on purpose but it must be total and stable: two
    direct boards both describing one posting is a real situation (a company
    migrating ATS), and a canonical version chosen by dict iteration order would
    change the score for no reason and be impossible to review.
    """
    merged: dict[str, str] = {}
    for state in state_maps:
        for namespace, version_id in state.items():
            merged.setdefault(namespace, version_id)
    if not merged:
        return None
    namespace = min(
        merged,
        key=lambda ns: (-CATEGORY_RANK.get(category_of(ns), 0), ns),
    )
    return namespace, merged[namespace]


# --------------------------------------------------------------------------- #
# The enrichment slot
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class EnrichmentOutcome:
    """What an enrichment stage learned about one posting.

    `description` is the body it obtained (or None); `identity` is that body's
    content identity, which is what the score is keyed on. `status` is the
    stage's own vocabulary ('available', 'unavailable', ...), carried through to
    the score's rationale and interpreted by nobody here.
    """

    description: str | None = None
    identity: str = ""
    status: str = "unknown"


@runtime_checkable
class EnrichmentStage(Protocol):
    """The seam Phase 3.2 plugs into. Called once per work batch.

    Deliberately a PROTOCOL and not an import. 3.2's description fetcher is being
    built alongside this, and a hard import would couple the two modules'
    lifecycles; a protocol slot lets the graph be complete, tested, and benchmarked
    before its enricher exists. It also means a run can legitimately choose not to
    enrich (a re-score pass over unchanged content has nothing to fetch).
    """

    def __call__(
        self, conn: sqlite3.Connection, items: Sequence[scoring.WorkRow]
    ) -> Mapping[str, EnrichmentOutcome]: ...


def null_enrichment(
    conn: sqlite3.Connection, items: Sequence[scoring.WorkRow]
) -> Mapping[str, EnrichmentOutcome]:
    """The default: enrich nothing, and say so by returning nothing.

    Not a stub that will one day be filled in -- it is the correct stage for a
    pass that is not fetching descriptions. Whatever is already on file is still
    read (`scoring.description_for`), so a description Phase 3.2 wrote yesterday
    scores today without this stage doing anything.
    """
    return {}


# --------------------------------------------------------------------------- #
# Reading the corpus
# --------------------------------------------------------------------------- #
#: A posting's last recorded content state. Same shape and same ordering rationale
#: as `runstore`'s per-batch lookup: chronological by the RUN's `requested_at`,
#: then the row's `recorded_at`, then `run_uid` to make the order total. Written
#: out here rather than reaching into `runstore`'s private helper so this module's
#: query is visible to its own EXPLAIN QUERY PLAN test.
_STATE_SQL = """
SELECT posting_id, source_state_json FROM (
    SELECT rp.posting_id AS posting_id,
           rp.source_state_json AS source_state_json,
           ROW_NUMBER() OVER (
               PARTITION BY rp.posting_id
               ORDER BY pr.requested_at DESC, rp.recorded_at DESC, rp.run_uid DESC
           ) AS rn
      FROM run_postings rp
      JOIN pipeline_runs pr ON pr.run_uid = rp.run_uid
     WHERE rp.posting_id IN ({placeholders})
       AND rp.source_state_json IS NOT NULL
) WHERE rn = 1
"""

#: Every posting the corpus has content state for, cursor-paginated. The
#: `posting_id > ?` predicate is ALWAYS supplied (empty string on the first page)
#: so this is a range SEARCH on `idx_run_postings_posting` rather than a scan of
#: `run_postings` -- a FULL pass reads the corpus in index order, one page at a
#: time, and never materialises it.
_CORPUS_SQL = """
SELECT DISTINCT rp.posting_id AS posting_id
  FROM run_postings rp
 WHERE rp.posting_id > ? AND rp.source_state_json IS NOT NULL
 ORDER BY rp.posting_id
"""

def _chunks(ids: Sequence[str], size: int = _LOOKUP_CHUNK) -> Iterable[list[str]]:
    ordered = list(dict.fromkeys(ids))
    for start in range(0, len(ordered), size):
        yield ordered[start:start + size]


def _state_maps(
    conn: sqlite3.Connection, posting_ids: Sequence[str]
) -> dict[str, list[dict[str, str]]]:
    """`{posting_id: [own state, ...states of postings redirecting into it]}`.

    The redirect-aware part is what makes a canonical match change the SCORE
    rather than only the bookkeeping: after an aggregator posting is redirected
    into a board posting, the board posting's canonical selection can see the
    aggregator's entry too -- and, more importantly, the board's own DIRECT entry
    now outranks it, so the tier stops being capped by the aggregator's undated,
    undescribed record.
    """
    targets = list(dict.fromkeys(posting_ids))
    incoming: dict[str, list[str]] = {}
    for chunk in _chunks(targets):
        for row in conn.execute(
            "SELECT from_posting_id, to_posting_id FROM posting_redirects "
            f"WHERE to_posting_id IN ({','.join('?' * len(chunk))})",
            chunk,
        ):
            incoming.setdefault(row["to_posting_id"], []).append(row["from_posting_id"])

    wanted = list(dict.fromkeys(targets + [p for v in incoming.values() for p in v]))
    states: dict[str, dict[str, str]] = {}
    for chunk in _chunks(wanted):
        sql = _STATE_SQL.format(placeholders=",".join("?" * len(chunk)))
        for row in conn.execute(sql, chunk):
            states[row["posting_id"]] = _load_state(row["source_state_json"])

    out: dict[str, list[dict[str, str]]] = {}
    for posting_id in targets:
        ordered = [states[posting_id]] if posting_id in states else []
        for source in sorted(incoming.get(posting_id, ())):
            if source in states:
                ordered.append(states[source])
        if ordered:
            out[posting_id] = ordered
    return out


def _load_state(blob: object) -> dict[str, str]:
    """Parse a stored state map; a malformed blob reads as no state.

    Degrading rather than raising, exactly as `runstore._load_state` does: the
    consequence of an unreadable state is that the posting looks unresolvable once
    and is skipped, where raising would take out the whole pass over one bad row.
    """
    if not blob:
        return {}
    try:
        loaded = json.loads(blob) if isinstance(blob, (str, bytes)) else None
    except (TypeError, ValueError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {str(k): str(v) for k, v in loaded.items()}


def _redirected_away(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> set[str]:
    """Postings that now resolve to something else, and are therefore not scored.

    A redirected posting is not deleted and not hidden -- its aliases, versions,
    and any score it already had all stay. It simply stops being the thing a score
    is about, because its identity now resolves to the survivor.
    """
    found: set[str] = set()
    for chunk in _chunks(posting_ids):
        for row in conn.execute(
            "SELECT from_posting_id FROM posting_redirects "
            f"WHERE from_posting_id IN ({','.join('?' * len(chunk))})",
            chunk,
        ):
            found.add(row["from_posting_id"])
    return found


def select_work(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    mode: PassMode,
    limit: int = DEFAULT_BATCH_SIZE,
    after: str | None = None,
) -> list[str]:
    """One page of the pass's work list, ordered by posting id.

    INCREMENTAL is the dirty set UNION the open invalidations. The union is what
    makes a canonical match reach the scorer at all: a resolved posting's CONTENT
    did not change (no source said anything new), so it is not dirty -- what
    changed is which version speaks for it, and that arrives as an invalidation.

    FULL is every posting with recorded content state, which is the honest
    definition of "the corpus this phase can reason about": a posting with no
    state was never observed by a source under Phase 3.1 and there is nothing to
    score it from.

    Both are cursor-paginated by `after` for `dirty_posting_ids`' reason -- `limit`
    alone re-returns the same first N, because this is a query and nothing here
    records that anyone consumed anything.
    """
    if mode is PassMode.FULL:
        sql = _CORPUS_SQL + " LIMIT ?"
        return [row["posting_id"] for row in conn.execute(sql, (after or "", int(limit)))]
    dirty = runstore.dirty_posting_ids(conn, run_uid, limit=limit, after=after)
    invalid = resolver.open_invalidations(conn, limit=limit, after=after)
    return sorted(set(dirty) | set(invalid))[:limit]


def build_work_rows(
    conn: sqlite3.Connection,
    posting_ids: Sequence[str],
    *,
    category_of: Callable[[str], SourceCategory] = registry_category,
) -> tuple[list[scoring.WorkRow], list[str]]:
    """Resolve a page of posting ids into exactly what the scorer needs.

    Returns `(rows, skipped)`. A posting is SKIPPED, never guessed at, when it has
    no recorded content state, when it redirects into another posting, or when the
    version its state names has gone missing. Each of those is a posting this phase
    genuinely cannot score, and scoring it from a default would put a fabricated
    tier in front of the user.

    Batched throughout: state maps, version rows, and descriptions are one
    statement per `_LOOKUP_CHUNK` ids each. Nothing here is per-posting I/O.
    """
    ids = list(dict.fromkeys(posting_ids))
    if not ids:
        return [], []

    skipped = _redirected_away(conn, ids)
    live = [p for p in ids if p not in skipped]
    states = _state_maps(conn, live)

    chosen: dict[str, tuple[str, str]] = {}
    for posting_id in live:
        selected = select_canonical_version(
            states.get(posting_id, ()), category_of=category_of
        )
        if selected is None:
            skipped.add(posting_id)
            continue
        chosen[posting_id] = selected

    version_rows: dict[str, sqlite3.Row] = {}
    for chunk in _chunks([v for _, v in chosen.values()]):
        for row in conn.execute(
            "SELECT posting_version_id, version_hash, title, company, location, salary, "
            "salary_min, salary_max, posted, remote, source, req_id FROM posting_versions "
            f"WHERE posting_version_id IN ({','.join('?' * len(chunk))})",
            chunk,
        ):
            version_rows[row["posting_version_id"]] = row

    descriptions = scoring.description_for(conn, list(chosen))

    rows: list[scoring.WorkRow] = []
    for posting_id in live:
        selected = chosen.get(posting_id)
        if selected is None:
            continue
        namespace, version_id = selected
        version = version_rows.get(version_id)
        if version is None:
            skipped.add(posting_id)
            continue
        body, identity = descriptions.get(posting_id, (None, ""))
        rows.append(
            scoring.WorkRow(
                posting_id=posting_id,
                posting_version_id=version_id,
                version_hash=version["version_hash"] or "",
                namespace=namespace,
                is_aggregator=category_of(namespace) is SourceCategory.AGGREGATOR,
                row=scoring.row_from_version(version),
                description=body,
                description_identity=identity,
            )
        )
    return rows, sorted(skipped)


# --------------------------------------------------------------------------- #
# Pass bookkeeping
# --------------------------------------------------------------------------- #
def baseline_run_uid(conn: sqlite3.Connection, run_uid: str) -> str | None:
    """The run `runstore.dirty_posting_ids(run_uid)` measures this run against.

    The newest CONSUMED run strictly older than this one, by the same ordering
    `runstore`'s dirty query uses. This is not a re-derivation for its own sake:
    the pass's INCREMENTAL work list is that dirty set, so the licence to trust it
    has to be checked against the very run it was measured from.
    """
    row = conn.execute(
        "SELECT requested_at FROM pipeline_runs WHERE run_uid=?", (run_uid,)
    ).fetchone()
    if row is None:
        return None
    previous = conn.execute(
        "SELECT run_uid FROM pipeline_runs WHERE status='succeeded' "
        "AND (requested_at, run_uid) < (?, ?) "
        "ORDER BY requested_at DESC, run_uid DESC LIMIT 1",
        (row["requested_at"], run_uid),
    ).fetchone()
    return None if previous is None else previous["run_uid"]


def baseline_pass(conn: sqlite3.Connection, run_uid: str) -> PassRecord | None:
    """The COMPLETED pass that licenses an incremental run, or None.

    Two conditions, both load-bearing:

      * it must be a pass on `baseline_run_uid(run_uid)` -- the exact run the
        dirty set is measured from. A completed pass on some OTHER run says
        nothing about the changes this run's dirty set omits.
      * it must have COMPLETED. A pass left running (the process died) or failed
        did not necessarily score what it selected, and its run may nonetheless
        have succeeded, so the next run's dirty set is already measured past it.
        Treating it as a baseline drops that work silently and forever; refusing
        to makes the next pass redo it.
    """
    baseline = baseline_run_uid(conn, run_uid)
    if baseline is None:
        return None
    row = conn.execute(
        "SELECT pass_id, run_uid, profile_version_id, scorer_hash, mode, status "
        "FROM score_passes WHERE run_uid=? AND status=? "
        "ORDER BY started_at DESC, pass_id DESC LIMIT 1",
        (baseline, PASS_COMPLETED),
    ).fetchone()
    if row is None:
        return None
    return PassRecord(
        pass_id=row["pass_id"], run_uid=row["run_uid"],
        profile_version_id=row["profile_version_id"], scorer_hash=row["scorer_hash"],
        mode=row["mode"], status=row["status"],
    )


@dataclass(frozen=True, slots=True)
class PassContext:
    """Everything the stages after `OpenPass` need, decided once."""

    pass_id: str
    run_uid: str
    identity: PassIdentity
    decision: PassDecision
    scorer: scoring.ScorerIdentity


@dataclass(slots=True)
class OpenPass:
    """Mint the profile version, decide the mode, open the `score_passes` row.

    Also fills `pipeline_runs.profile_version_id`, which has been NULL for every
    scheduler run since migration 6 declared it: a run that scored anything was
    always scored against SOME profile, and leaving the column empty meant the
    only record of which one lived inside the score rows.

    Not frozen, for `writer.MarkPresence`'s reason: the context only exists once
    the SQL has run, so `apply` publishes it by ASSIGNING `self.context` and
    `self.events`.
    """

    run_uid: str
    at: str
    profile_doc: object
    scorer: scoring.ScorerIdentity
    events: tuple[RunEvent, ...] = ()
    context: PassContext | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> PassContext:
        profile_version_id = scoring.upsert_profile_version(conn, self.profile_doc, at=self.at)
        conn.execute(
            "UPDATE pipeline_runs SET profile_version_id=? WHERE run_uid=?",
            (profile_version_id, self.run_uid),
        )
        identity = PassIdentity(
            profile_version_id=profile_version_id, scorer_hash=self.scorer.scorer_hash
        )
        decision = decide_mode(identity, baseline_pass(conn, self.run_uid))
        pass_id = str(
            uuid.uuid5(
                _PASS_NAMESPACE,
                runstore.canonical_json(
                    [self.run_uid, profile_version_id, self.scorer.scorer_hash]
                ),
            )
        )
        # Deterministic id plus an upsert: re-entering a run under the same
        # identity RESUMES one pass rather than forking a second, and the counters
        # reset because a pass that is being re-run has not scored anything yet.
        conn.execute(
            "INSERT INTO score_passes "
            "(pass_id, run_uid, profile_version_id, scorer_hash, mode, status, started_at) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT (run_uid, profile_version_id, scorer_hash) DO UPDATE SET "
            "mode=excluded.mode, status=excluded.status, started_at=excluded.started_at, "
            "finished_at=NULL, selected=0, scored=0, reused=0, skipped=0, report_json=NULL",
            (pass_id, self.run_uid, profile_version_id, self.scorer.scorer_hash,
             str(decision.mode), PASS_RUNNING, self.at),
        )
        context = PassContext(
            pass_id=pass_id, run_uid=self.run_uid, identity=identity,
            decision=decision, scorer=self.scorer,
        )
        self.context = context
        self.events = (
            RunEvent(
                run_uid=self.run_uid,
                event_type="score.pass_opened",
                at=self.at,
                payload={
                    "pass_id": pass_id,
                    "mode": str(decision.mode),
                    "reasons": list(decision.reasons),
                    "profile_version_id": profile_version_id,
                    "scorer_hash": self.scorer.scorer_hash,
                    "rubric_version": self.scorer.rubric_version,
                },
            ),
        )
        return context


@dataclass(slots=True)
class ClosePass:
    """Settle the pass row and consume the invalidations it satisfied.

    Consumption happens HERE and only here. An invalidation consumed when it was
    selected would be lost if the pass then died; consumed on completion, a dead
    pass leaves its work queued for the next one. FULL mode consumes every open
    row because it covered every posting; INCREMENTAL consumes exactly the
    postings it selected.
    """

    run_uid: str
    at: str
    context: PassContext
    report: Mapping[str, object]
    status: str = PASS_COMPLETED
    selected_posting_ids: tuple[str, ...] = ()
    events: tuple[RunEvent, ...] = ()
    consumed: int = 0

    def apply(self, conn: sqlite3.Connection) -> dict:
        report = dict(self.report)
        if self.status == PASS_COMPLETED:
            self.consumed = resolver.consume_invalidations(
                conn,
                run_uid=self.run_uid,
                at=self.at,
                posting_ids=None
                if self.context.decision.mode is PassMode.FULL
                else list(self.selected_posting_ids),
            )
            report["invalidations_consumed"] = self.consumed
        conn.execute(
            "UPDATE score_passes SET status=?, finished_at=?, selected=?, scored=?, "
            "reused=?, skipped=?, report_json=? WHERE pass_id=?",
            (self.status, self.at, int(report.get("selected", 0)),
             int(report.get("scored", 0)), int(report.get("reused", 0)),
             int(report.get("skipped", 0)), runstore.canonical_json(report),
             self.context.pass_id),
        )
        self.events = (
            RunEvent(
                run_uid=self.run_uid,
                event_type="score.pass_completed"
                if self.status == PASS_COMPLETED
                else "score.pass_failed",
                at=self.at,
                payload=dict(report),
            ),
        )
        return report


# --------------------------------------------------------------------------- #
# The driver
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class ResolvePass:
    """The resolve stage: one run's direct index, one run's aggregator subjects.

    RUN-SCOPED, NOT PAGE-SCOPED, and that is a deliberate departure from doing this
    per work page. The subject list is this run's AGGREGATOR observations rather
    than the aggregator postings in the work list, because the case that matters
    most is precisely the one a work-list restriction misses: an aggregator mirror
    seen last week, and the BOARD catching up to it today. The mirror's content did
    not move, so it is not dirty and would never appear in an incremental work list
    -- and the merge that makes the ghost cap stop firing would never happen.

    The cost argument the work-list restriction was protecting is preserved: both
    sides come from ONE run-scoped query over `run_postings`, there is no corpus
    scan and no new database index, and when the run observed no aggregator
    postings at all the index is never built and the stage does nothing.

    Postings that already carry a redirect are excluded: a posting resolves once,
    and re-deciding it later would silently rewrite a merge a user may have acted
    on.

    Not frozen, for `writer.MarkPresence`'s reason: `apply` publishes its report by
    ASSIGNING `self.report` / `self.events`.
    """

    run_uid: str
    at: str
    category_of: Callable[[str], SourceCategory] = registry_category
    events: tuple[RunEvent, ...] = ()
    report: dict | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> dict:
        observations = resolver.run_observations(conn, run_uid=self.run_uid)
        subjects_all = [
            o for o in observations
            if self.category_of(o.namespace) is SourceCategory.AGGREGATOR
        ]
        empty = {"subjects": 0, "indexed_direct": 0, "matched": (), "ambiguous": 0,
                 "invalidated": ()}
        if not subjects_all:
            self.report = empty
            return empty
        already = _redirected_away(conn, [o.posting_id for o in subjects_all])
        subjects = [
            (o.posting_id, o.company, o.title, o.location)
            for o in subjects_all if o.posting_id not in already
        ]
        if not subjects:
            self.report = empty
            return empty
        index = resolver.build_direct_index(
            o for o in observations if self.category_of(o.namespace) in resolver.DIRECT_CATEGORIES
        )
        op = resolver.ResolveAggregators(
            run_uid=self.run_uid, at=self.at, subjects=tuple(subjects), index=index,
            category_of=self.category_of,
        )
        report = op.apply(conn)
        self.report = report
        self.events = op.events
        return report


def _apply_enrichment(
    row: scoring.WorkRow, outcome: EnrichmentOutcome | None
) -> scoring.WorkRow:
    """Fold one enrichment result into a work row, or leave the row untouched.

    An enricher that says nothing about a posting leaves whatever
    `scoring.description_for` already read on file, which is what makes
    `null_enrichment` a correct stage rather than a data-losing one.
    """
    if outcome is None:
        return row
    return scoring.WorkRow(
        posting_id=row.posting_id,
        posting_version_id=row.posting_version_id,
        version_hash=row.version_hash,
        namespace=row.namespace,
        is_aggregator=row.is_aggregator,
        row=row.row,
        description=outcome.description,
        description_identity=outcome.identity,
    )


@dataclass(slots=True)
class ScoreGraphPass:
    """One page of work driven through resolve -> enrich -> score, as one op.

    Composed of the stage ops rather than reimplementing them, so the sequence a
    Phase 4 writer submits and the sequence a test drives are the same code.
    """

    run_uid: str
    at: str
    context: PassContext
    posting_ids: tuple[str, ...]
    category_of: Callable[[str], SourceCategory] = registry_category
    enrichment: EnrichmentStage = null_enrichment
    source_run_id: str | None = None
    events: tuple[RunEvent, ...] = ()
    report: dict | None = field(default=None, compare=False)

    def apply(self, conn: sqlite3.Connection) -> dict:
        events: list[RunEvent] = []
        ids = list(self.posting_ids)

        # -- build the work rows (redirect-aware) --------------------------- #
        rows, skipped = build_work_rows(conn, ids, category_of=self.category_of)

        # -- enrichment slot ------------------------------------------------ #
        enriched = dict(self.enrichment(conn, rows)) if rows else {}
        if enriched:
            rows = [_apply_enrichment(row, enriched.get(row.posting_id)) for row in rows]

        # -- score ---------------------------------------------------------- #
        op = scoring.ScoreWork(
            run_uid=self.run_uid, at=self.at, items=tuple(rows),
            profile_version_id=self.context.identity.profile_version_id,
            scorer=self.context.scorer, source_run_id=self.source_run_id,
        )
        outcome = op.apply(conn)
        events.extend(op.events)

        report = {
            "selected": len(self.posting_ids),
            "scored": outcome.scored,
            "reused": outcome.reused,
            "superseded": outcome.superseded,
            "blocked": outcome.blocked,
            "skipped": len(skipped),
            "posting_ids": tuple(ids),
        }
        self.report = report
        self.events = tuple(events)
        return report


def run_pass(
    conn: sqlite3.Connection,
    *,
    run_uid: str,
    profile_doc,
    at: str | None = None,
    scorer: scoring.ScorerIdentity | None = None,
    category_of: Callable[[str], SourceCategory] = registry_category,
    enrichment: EnrichmentStage = null_enrichment,
    batch_size: int = DEFAULT_BATCH_SIZE,
    bridge: bool = True,
    source_run_id: str | None = None,
) -> dict[str, object]:
    """Drive a whole scoring pass for one run against a caller-owned connection.

    The stage order is the graph's, and the first three run ONCE for the whole
    pass because each of them can change what the pages after it must look at:

      1. the normalization bridge, which can mint the very redirects step 2 and
         step 3 have to see;
      2. the direct-inventory index and aggregator resolution, which emit
         invalidations for their survivors;
      3. the work list, page by page: build rows (redirect-aware, so a survivor
         sees the state maps merged into it), enrich, score;
      4. settle the pass row and consume the invalidations it satisfied.

    Steps 1 and 2 emit invalidations BEFORE the work list is read, which is what
    puts their survivors into it: a resolved posting's content did not change, so
    it is not dirty -- what changed is which version speaks for it.

    No transaction control, like everything else in this layer. A Phase 4 caller
    submits the same ops through `writer.submit` and gets the same result with the
    writer's batching and durability; this function is what the tests and the
    benchmark drive, and what a one-shot re-score would call directly.
    """
    stamp = at or runstore.utc_now_iso()
    identity = scorer or scoring.scorer_identity()

    opener = OpenPass(run_uid=run_uid, at=stamp, profile_doc=profile_doc, scorer=identity)
    context = opener.apply(conn)

    bridge_report: dict[str, object] = {}
    if bridge:
        bridge_op = resolver.BridgeLegacyUrls(run_uid=run_uid, at=stamp)
        bridge_report = bridge_op.apply(conn)

    resolve_op = ResolvePass(run_uid=run_uid, at=stamp, category_of=category_of)
    resolve_report = resolve_op.apply(conn)

    totals = {"selected": 0, "scored": 0, "reused": 0, "superseded": 0,
              "blocked": 0, "skipped": 0}
    touched: list[str] = []
    cursor: str | None = None
    while True:
        page = select_work(
            conn, run_uid=run_uid, mode=context.decision.mode, limit=batch_size, after=cursor
        )
        if not page:
            break
        stage = ScoreGraphPass(
            run_uid=run_uid, at=stamp, context=context, posting_ids=tuple(page),
            category_of=category_of, enrichment=enrichment, source_run_id=source_run_id,
        )
        report = stage.apply(conn)
        for key in totals:
            totals[key] += int(report[key])
        touched.extend(report["posting_ids"])
        cursor = page[-1]

    summary = {
        "run_uid": run_uid,
        "pass_id": context.pass_id,
        "mode": str(context.decision.mode),
        "reasons": list(context.decision.reasons),
        "profile_version_id": context.identity.profile_version_id,
        "scorer_hash": context.scorer.scorer_hash,
        "rubric_version": context.scorer.rubric_version,
        "bridge": {k: v for k, v in bridge_report.items() if k != "invalidated"},
        "resolved": len(resolve_report["matched"]),
        "resolve_ambiguous": int(resolve_report["ambiguous"]),
        **totals,
    }
    closer = ClosePass(
        run_uid=run_uid, at=stamp, context=context, report=summary,
        selected_posting_ids=tuple(dict.fromkeys(touched)),
    )
    return closer.apply(conn)
