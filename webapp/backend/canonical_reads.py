"""Canonical read functions: jobs / changes / analytics / freshness / details,
served from the canonical `postings` + current-score tables (`score_versions`,
`posting_versions`, `descriptions`, `run_postings`, `pipeline_runs`), DTO-shape
compatible with the legacy `/api/*` read endpoints (routers/jobs.py, changes.py,
analytics.py; DTO field names pinned by frontend/src/api/types.ts).

Every function here takes an explicit `sqlite3.Connection` and only ever reads:
no writes, no commits, no module-level caching. Callers (routers/readsv2.py) own
connection lifecycle exactly like the legacy routers do via `db.get_db`.

DESIGN NOTES (see the phase 4 spec, W-4.2, for the constraints these answer)

CANONICAL VERSION SELECTION is reused, not re-derived: `sources.graph.
select_canonical_version` (rank DIRECT/STARTUP_BOARD > MANUAL > AGGREGATOR, ties
broken lexicographically by namespace, redirect-aware) and `sources.graph.
registry_category` are imported verbatim from the scoring graph, so a posting's
canonical version here is picked by the exact rule the scorer used to decide what
to score. `registry_category` needs the adapter registry populated to rank
anything above AGGREGATOR, so this module imports `sources.adapters` purely for
its import-time `install()` side effect (idempotent -- see that module).

This module supplies `select_canonical_version`'s input (`state_maps`, i.e. each
posting's `{namespace: posting_version_id}` from `run_postings.source_state_json`,
redirect-aware) with its own SQL rather than importing graph's private per-pass
helpers (`_state_maps` etc): those are shaped for a scoring PASS -- cursor pages,
a `posting_id` allowlist to resolve -- not for an arbitrary read, and duplicating
their two small queries here is cheaper than bending that shape to fit.

FALLBACK CHAIN for a posting's canonical version, in order:
  1. `select_canonical_version` over the posting's (and its redirect sources')
     state maps -- the live, scheduler-observed answer, and the same one the
     scorer used.
  2. the newest `version_kind='legacy-current'` row (migration 11's backfill) --
     for a posting the scheduler has never touched, which has no state map at
     all (legacy `run_postings` rows carry no `source_state_json`).
  3. the newest row of any kind -- a last resort so a posting with SOME content
     on file is never silently dropped from a listing.
A posting with no `posting_versions` row at all (should not happen once a
posting is minted) contributes nothing and is skipped rather than fabricated.

CANONICAL DISPLAY URL is independent of version selection: the newest active
(`valid_to IS NULL`) `posting_aliases` row that carries a URL (`url IS NOT
NULL`), ordered `valid_from DESC, alias_id DESC`. This is the EXACT rule
`compat_jobs` (migrations.py) already uses for legacy parity; canonical writes
populate `posting_aliases.url` on every claim (`runstore._insert_alias`), not
only `alias_kind='url'` ones, so one rule covers requisition-alias postings too.
For a HISTORICAL url (the `changes` endpoint's `disappeared` list, which needs
"the url as of the baseline run" rather than "the url now"), the temporal variant
adds the alias-validity window `valid_from <= at AND (valid_to IS NULL OR
valid_to > at)`, mirroring `compat_job_history`'s identical window.

CURRENT SCORE: `score_versions` joined through `posting_version_id`, filtered
`superseded_at IS NULL`. More than one row can legally be current for the same
version (supersession is keyed by (posting_version_id, profile_version_id,
scorer_hash), so two profiles -- or a reverted input, which re-currents its OLD
row without touching `created_at` -- can both be current at once); the winner
is `created_at DESC, score_version_id DESC`, a fully deterministic key chosen
specifically so equal `created_at` values (the revert case) do not fall back to
undefined SQL row order. A profile-identity-aware tiebreak (prefer whichever
row's `profile_version_id` matches the most recent scoring pass) would resolve
the revert scenario more precisely than a lexicographic id ever can, but is not
cheaply available here -- see `_current_scores`'s docstring. Never filtered by
the nullable `score_versions.posting_id` column alone -- migration 19 explicitly
does not backfill it for migration 11's legacy rows, so an `posting_id=`
predicate would silently miss every legacy-imported score.

`why` / `flags` / `odds_why` all come from the CURRENT score's `rationale_json`
(both the live scorer, `scoring.persist_scores`, and migration 11's legacy
backfill write those exact three keys there -- verified against both writers).
`flags` differs in shape between the two: migration 11 stores the already
comma-joined legacy string, the live scorer stores a JSON list. `_flags_str`
normalizes either to the string legacy `JobLight.flags` expects.

JOB_STATE JOIN: `job_state` by `posting_id` first, `url` second (`job_state.
posting_id` is nullable -- Phase 4's write side has not moved yet, so most rows
in a database still populated by legacy writes only carry `url`).

TIER-AT-A-PAST-RUN (used by `changes()` for `tier_changed`/`disappeared`) is a
best-effort reconstruction, not a stored fact: unlike legacy `job_history`, no
canonical table snapshots "this posting's tier as of run N" directly. `run_
postings.posting_version_id` names the version linked to a posting in a given
run; `_tier_for_version` reads that version's EARLIEST `score_versions` row
(content is immutable per version, so there is normally exactly one score ever
computed for it) as the tier "as of" that version, falling back to `posting_
versions.tier` for legacy rows scored before Phase 3.3 existed. This is exact
for the common case and does not fabricate a value it cannot derive (falls back
to None).

RUN MEMBERSHIP (`_run_membership`, used by `changes()`) filters `run_postings.
membership_kind='snapshot'`: migration 11's 'current-only' rows assert "this
posting's CURRENT state" as of the migration, not "this posting was present in
THIS run", and `compat_job_history` excludes them for exactly that reason
(migrations.py). Without the filter a legacy-backfilled posting would appear to
have been a member of every completed run, which is not what was observed.

SALARY / FIRST-SEEN / ALSO-SEEN-ON RECOVERY. `runstore._link_source_version`
(the scheduler's write path) never populates `posting_versions.salary_min`,
`salary_max`, `first_seen`, or `also_seen_on` -- those are Phase 1/legacy
concepts with no scheduler writer, so every canonically-written (non-migrated)
posting has them NULL. `build_light_rows` recovers them at READ time, never at
write time: `first_seen` COALESCEs with `postings.first_seen_at`, exactly the
rule `compat_jobs` already uses (migrations.py); `salary`/`posted`/`remote`
fall back to `posting_versions.payload_json`'s `canonical` sub-object (the same
raw fields `NormalizedPosting.canonical_fields()` wrote there) when the column
itself is NULL; `salary_min`/`salary_max` have no structured source anywhere
(the payload only ever carried the raw salary TEXT, never parsed numbers), so
this module recovers them by parsing that text with `_parse_salary_range`, a
small regex ported from `scraper.parse_salary` rather than imported from it
(see that function's docstring for why). `also_seen_on` is derived from the
posting's OWN state-map namespaces (the same `{namespace: posting_version_id}`
data `_canonical_versions` already reads) minus the canonical namespace --
cheap, deterministic, and exactly "which other sources also carry this
identical posting", the legacy field's meaning.

REPOSTED (`changes()`'s `reposted` list) has NO canonical equivalent of legacy
`cmd_score`'s on-disk seen-key ledger, and the canonical scorer deliberately
never emits a `reposted` flag (`sources/scoring.py`'s `_score_one` docstring).
Rather than fake the signal from a flags string the scorer never writes, this
module derives it from `postings.returned_at` (Phase 2.4's sticky
absent-then-returned marker): a posting counts as reposted in a `changes()`
window when its `returned_at` falls strictly after the baseline run's
`recorded_at_hint` and no later than the current run's -- i.e. it went absent
and came back inside the exact window being compared. `returned_at` itself is
sticky (never cleared on a later absence), which is exactly why the window
bound matters: without it, a posting that returned once years ago would read
as "reposted" in every `changes()` call forever.

CROSS-ENDPOINT KEY CONSISTENCY. `JobLight.seen_key` is repurposed here to carry
`posting_id` (never a legacy `job_state.seen_key`), and the legacy `/api/funnel`
/ `/api/activity` endpoints still aggregate on `job_state.seen_key` / `url`. The
two key spaces do not collide today because nothing compares them directly, but
a future phase that flips reads to this module must reconcile them before any
cross-endpoint aggregation is trusted.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable, Sequence

from . import config
from .models import JobLight, JobState, date_plus, today_iso, url_to_b64
from .sources import adapters  # noqa: F401 -- import-time install() side effect
from .sources import graph, runstore
from .sweeprunner import runner

__all__ = [
    "analytics",
    "build_light_rows",
    "changes",
    "followups",
    "freshness",
    "job_detail",
    "list_jobs",
]

#: Matches sources.runstore._LOOKUP_CHUNK / sources.graph._LOOKUP_CHUNK -- one
#: IN-clause statement per this many ids, never one statement per row.
_CHUNK = 400

#: pipeline_runs rows that represent a completed, trustworthy run for read
#: purposes -- 'succeeded'/'partial' from the live scheduler, 'imported' from
#: the legacy migration. Never 'failed'/'cancelled', and never a run still
#: 'running' (its run_postings membership is not final).
_COMPLETED_RUN_STATUSES = ("succeeded", "partial", "imported")

#: The competition axis (Phase 3.5): odds stores the combined "<match> /
#: <competition>" string rubric.hireability() emits. Mirrors routers/analytics.
#: py's _COMPETITION / _competition_of exactly (duplicated rather than imported
#: so this module has no dependency on a legacy router file).
_COMPETITION = ["High competition", "Standard", "Lower bar"]


def _competition_of(odds: str | None) -> str | None:
    if not odds or " / " not in odds:
        return None
    return odds.split(" / ", 1)[1]


def _chunks(ids: Iterable[str], size: int = _CHUNK) -> Iterable[list[str]]:
    ordered = list(dict.fromkeys(ids))
    for start in range(0, len(ordered), size):
        yield ordered[start:start + size]


def _in_clause(n: int) -> str:
    return ",".join("?" * n)


def _load_state_json(blob: object) -> dict[str, str]:
    if not blob:
        return {}
    try:
        loaded = json.loads(blob) if isinstance(blob, (str, bytes)) else None
    except (TypeError, ValueError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {str(k): str(v) for k, v in loaded.items()}


def _flags_str(value: object) -> str | None:
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    if isinstance(value, str):
        return value
    return None


def _rationale(rationale_json: str | None) -> dict:
    if not rationale_json:
        return {}
    try:
        parsed = json.loads(rationale_json)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _payload_canonical(payload_json: str | None) -> dict:
    """The `canonical` sub-object of a `posting_versions.payload_json` blob
    (`{"canonical": {...}, "source": {...}, "first_observed": {...}}`, written
    by `runstore._link_source_version` from `NormalizedPosting.canonical_
    fields()`) -- the fallback source for `salary`/`posted`/`remote` when the
    version's own column is NULL. Malformed/missing payload yields {}."""
    if not payload_json:
        return {}
    try:
        parsed = json.loads(payload_json)
    except (TypeError, ValueError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    canonical = parsed.get("canonical")
    return canonical if isinstance(canonical, dict) else {}


#: (min annual salary, max annual salary) plausibility band `_parse_salary_range`
#: keeps a parsed number inside -- below this a number is probably a level/req
#: id fragment, above it probably a typo. Matches `scraper.parse_salary`'s band.
_SALARY_PLAUSIBLE = (25_000, 900_000)


def _parse_salary_range(text: str | None) -> tuple[int | None, int | None]:
    """Annual (lo, hi) recovered from free-text salary, or (None, None).

    Deliberately DUPLICATES `scraper.parse_salary`'s algorithm (hourly rates
    annualized at 2080 hr/yr, a trailing "k" times a thousand, a plausibility
    band that rejects level numbers and typos) rather than importing it:
    `scraper.py` is a bare top-level script (module-level `requests` import,
    `argparse` wiring, a hard dependency on `config.json` existing) pulled in
    at process start, not a library meant to be imported into a read path for
    one pure function -- and this module's own precedent (see the module
    docstring's canonical-version-selection note) is to duplicate a SMALL,
    stable piece of logic rather than bend an unrelated module's shape to fit
    a read. Keep the two in sync by hand if either changes; this module's own
    tests pin `_parse_salary_range`'s behavior directly rather than diffing it
    against `scraper.parse_salary`.

    This is the ONLY place a canonically-written posting's `salary_min`/
    `salary_max` numbers come from: `NormalizedPosting` never carried them
    (only raw `salary_text`), and `runstore._link_source_version` never
    computed them, so nothing durable has ever parsed this text before now.
    """
    if not text:
        return None, None
    txt = str(text).lower().replace(",", "")
    txt = re.sub(r"401\s*\(?k\)?", " ", txt)
    hourly = bool(re.search(r"hour|hourly|/hr\b|\bhr\b", txt))
    raw = re.findall(r"\$?\s*(\d{1,7}(?:\.\d+)?)\s*(k)?", txt)
    any_k = any(k for _m, k in raw)
    nums: list[float] = []
    for m, k in raw:
        try:
            v = float(m)
        except ValueError:
            continue
        if k:
            v *= 1000
        elif hourly and v < 500:
            v *= 2080
        elif any_k and 20 <= v <= 999:
            v *= 1000
        nums.append(v)
    lo_bound, hi_bound = _SALARY_PLAUSIBLE
    vals = [v for v in nums if lo_bound <= v <= hi_bound]
    if not vals:
        return None, None
    return int(min(vals)), int(max(vals))


# --------------------------------------------------------------------------- #
# Canonical version selection
# --------------------------------------------------------------------------- #
def _own_state_maps(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, dict[str, str]]:
    """{posting_id: {namespace: posting_version_id}} from the latest run_postings
    row (by pipeline_runs.requested_at, run_postings.recorded_at, run_uid) that
    carries a non-null source_state_json. Same shape/ordering as sources.graph.
    _STATE_SQL / sources.runstore._CURRENT_STATE_SQL (see module docstring for
    why this is a fresh query rather than an import of either)."""
    out: dict[str, dict[str, str]] = {}
    for chunk in _chunks(posting_ids):
        sql = f"""
            SELECT posting_id, source_state_json FROM (
                SELECT rp.posting_id AS posting_id,
                       rp.source_state_json AS source_state_json,
                       ROW_NUMBER() OVER (
                           PARTITION BY rp.posting_id
                           ORDER BY pr.requested_at DESC, rp.recorded_at DESC, rp.run_uid DESC
                       ) AS rn
                  FROM run_postings rp
                  JOIN pipeline_runs pr ON pr.run_uid = rp.run_uid
                 WHERE rp.posting_id IN ({_in_clause(len(chunk))})
                   AND rp.source_state_json IS NOT NULL
            ) WHERE rn = 1
        """
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = _load_state_json(row["source_state_json"])
    return out


def _incoming_redirects(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for chunk in _chunks(posting_ids):
        sql = (
            "SELECT from_posting_id, to_posting_id FROM posting_redirects "
            f"WHERE to_posting_id IN ({_in_clause(len(chunk))})"
        )
        for row in conn.execute(sql, chunk):
            out.setdefault(row["to_posting_id"], []).append(row["from_posting_id"])
    return out


def _canonical_versions(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, tuple[str, str]]:
    """{posting_id: (namespace, posting_version_id)} via graph.select_canonical_
    version, reused verbatim from the scoring graph."""
    ids = list(dict.fromkeys(posting_ids))
    if not ids:
        return {}
    own = _own_state_maps(conn, ids)
    incoming = _incoming_redirects(conn, ids)
    extra_ids = sorted({fid for froms in incoming.values() for fid in froms})
    extra = _own_state_maps(conn, extra_ids) if extra_ids else {}

    result: dict[str, tuple[str, str]] = {}
    for pid in ids:
        state_maps = []
        if pid in own:
            state_maps.append(own[pid])
        for fid in sorted(incoming.get(pid, ())):
            if fid in extra:
                state_maps.append(extra[fid])
        if not state_maps:
            continue
        chosen = graph.select_canonical_version(state_maps, category_of=graph.registry_category)
        if chosen is not None:
            result[pid] = chosen
    return result


def _all_namespaces(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, set[str]]:
    """{posting_id: {namespace, ...}} -- the union of the posting's own state-
    map namespaces and any redirect-source postings' state-map namespaces. The
    same state-map data `_canonical_versions` selects from, re-queried here
    (rather than threaded through that function's return shape, which a test
    pins as a plain `(namespace, posting_version_id)` 2-tuple) so `also_seen_on`
    can report every source that carries this posting's identity, not only the
    one `select_canonical_version` picked."""
    ids = list(dict.fromkeys(posting_ids))
    if not ids:
        return {}
    own = _own_state_maps(conn, ids)
    incoming = _incoming_redirects(conn, ids)
    extra_ids = sorted({fid for froms in incoming.values() for fid in froms})
    extra = _own_state_maps(conn, extra_ids) if extra_ids else {}

    out: dict[str, set[str]] = {}
    for pid in ids:
        namespaces: set[str] = set(own.get(pid, {}))
        for fid in incoming.get(pid, ()):
            namespaces |= set(extra.get(fid, {}))
        out[pid] = namespaces
    return out


def _partition_latest(conn: sqlite3.Connection, posting_ids: Sequence[str], *, where: str) -> dict[str, str]:
    """{posting_id: posting_version_id} for the newest posting_versions row per
    posting (observed_at DESC, posting_version_id DESC), optionally filtered."""
    out: dict[str, str] = {}
    for chunk in _chunks(posting_ids):
        sql = f"""
            SELECT posting_id, posting_version_id FROM (
                SELECT posting_id, posting_version_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY posting_id ORDER BY observed_at DESC, posting_version_id DESC
                       ) AS rn
                  FROM posting_versions
                 WHERE posting_id IN ({_in_clause(len(chunk))}) {where}
            ) WHERE rn = 1
        """
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = row["posting_version_id"]
    return out


def _version_id_for_postings(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, str]:
    """posting_id -> posting_version_id, following the fallback chain documented
    in the module docstring."""
    ids = list(dict.fromkeys(posting_ids))
    canonical = _canonical_versions(conn, ids)
    result = {pid: vid for pid, (_ns, vid) in canonical.items()}
    missing = [p for p in ids if p not in result]
    if missing:
        legacy = _partition_latest(conn, missing, where="AND version_kind='legacy-current'")
        result.update(legacy)
        missing2 = [p for p in missing if p not in legacy]
        if missing2:
            result.update(_partition_latest(conn, missing2, where=""))
    return result


_VERSION_COLS = (
    "posting_version_id, posting_id, version_kind, version_hash, observed_at, "
    "title, company, location, salary, salary_min, salary_max, posted, remote, "
    "source, req_id, tier, odds, odds_score, odds_why, first_seen, "
    "also_seen_on, desc_snippet, why, flags, payload_json"
)


def _version_rows(conn: sqlite3.Connection, version_ids: Sequence[str]) -> dict[str, sqlite3.Row]:
    out: dict[str, sqlite3.Row] = {}
    for chunk in _chunks(version_ids):
        sql = f"SELECT {_VERSION_COLS} FROM posting_versions WHERE posting_version_id IN ({_in_clause(len(chunk))})"
        for row in conn.execute(sql, chunk):
            out[row["posting_version_id"]] = row
    return out


def _current_score_key(row: sqlite3.Row) -> tuple[str, str]:
    return (row["created_at"] or "", row["score_version_id"] or "")


def _current_scores(conn: sqlite3.Connection, version_ids: Sequence[str]) -> dict[str, sqlite3.Row]:
    """version_id -> the current (superseded_at IS NULL) score_versions row for
    that version, deterministically chosen when more than one is legally
    current (see module docstring's CURRENT SCORE note). The key is `created_
    at DESC, score_version_id DESC` -- `created_at` alone is not enough because
    `persist_scores`'s "reverting inputs" step re-currents an OLD row without
    touching its `created_at`, so two current rows can tie on it, and without a
    second key the winner would silently depend on SQL row order. A profile-
    identity-aware tiebreak (prefer the row whose `profile_version_id` matches
    whatever profile the most recent scoring pass actually used, so a profile
    revert cannot leave a wrong-profile score winning purely because it happens
    to have a newer `created_at`) is not implemented: nothing on this row, nor
    on any row this module already fetches in bulk, cheaply names "the profile
    currently in force" -- `pipeline_runs.profile_version_id` exists but would
    need a per-candidate join back through `source_run_id`, and is itself
    optional and inconsistently populated. See module docstring for why this
    joins through posting_version_id rather than filtering score_versions.
    posting_id."""
    best: dict[str, sqlite3.Row] = {}
    for chunk in _chunks(version_ids):
        sql = (
            "SELECT score_version_id, posting_version_id, tier, odds, odds_score, "
            "rationale_json, created_at "
            "FROM score_versions WHERE superseded_at IS NULL "
            f"AND posting_version_id IN ({_in_clause(len(chunk))})"
        )
        for row in conn.execute(sql, chunk):
            vid = row["posting_version_id"]
            current = best.get(vid)
            if current is None or _current_score_key(row) > _current_score_key(current):
                best[vid] = row
    return best


def _display_urls(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for chunk in _chunks(posting_ids):
        sql = f"""
            SELECT posting_id, url FROM (
                SELECT posting_id, url,
                       ROW_NUMBER() OVER (
                           PARTITION BY posting_id ORDER BY valid_from DESC, alias_id DESC
                       ) AS rn
                  FROM posting_aliases
                 WHERE valid_to IS NULL AND url IS NOT NULL
                   AND posting_id IN ({_in_clause(len(chunk))})
            ) WHERE rn = 1
        """
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = row["url"]
    return out


def _url_as_of(conn: sqlite3.Connection, posting_id: str, at: str | None) -> str | None:
    """The alias url active for `posting_id` at instant `at`, mirroring compat_
    job_history's temporal-window rule. None if `at` is unknown."""
    if at is None:
        return None
    row = conn.execute(
        "SELECT url FROM posting_aliases WHERE posting_id=? AND url IS NOT NULL "
        "AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?) "
        "ORDER BY valid_from DESC, alias_id DESC LIMIT 1",
        (posting_id, at, at),
    ).fetchone()
    return row["url"] if row else None


def _posting_id_for_url(conn: sqlite3.Connection, url: str) -> str | None:
    row = conn.execute(
        "SELECT posting_id FROM posting_aliases WHERE url=? AND valid_to IS NULL "
        "ORDER BY valid_from DESC, alias_id DESC LIMIT 1",
        (url,),
    ).fetchone()
    return row["posting_id"] if row else None


def _latest_descriptions(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, sqlite3.Row]:
    out: dict[str, sqlite3.Row] = {}
    for chunk in _chunks(posting_ids):
        sql = f"""
            SELECT posting_id, body, fetch_status FROM (
                SELECT posting_id, body, fetch_status,
                       ROW_NUMBER() OVER (
                           PARTITION BY posting_id ORDER BY fetched_at DESC, description_id DESC
                       ) AS rn
                  FROM descriptions
                 WHERE posting_id IN ({_in_clause(len(chunk))})
            ) WHERE rn = 1
        """
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = row
    return out


# --------------------------------------------------------------------------- #
# job_state
# --------------------------------------------------------------------------- #
_STATE_COLS = (
    "seen_key, url, posting_id, status, notes, follow_up_date, applied_date, "
    "starred, hidden, contact, snoozed_until, applied_via, updated_at"
)


def _state_by_posting(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, sqlite3.Row]:
    out: dict[str, sqlite3.Row] = {}
    for chunk in _chunks(posting_ids):
        sql = f"SELECT {_STATE_COLS} FROM job_state WHERE posting_id IN ({_in_clause(len(chunk))})"
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = row
    return out


def _state_by_url(conn: sqlite3.Connection, urls: Sequence[str]) -> dict[str, sqlite3.Row]:
    """{url: job_state row}, the most-recently-updated row winning when several
    `job_state` rows share a url (no uniqueness constraint on `url` -- only
    `seen_key` is a primary key). `ORDER BY updated_at DESC, seen_key DESC`
    makes that winner deterministic instead of leaving it to SQL row order;
    `setdefault` below then keeps the FIRST row seen per url, i.e. the newest."""
    out: dict[str, sqlite3.Row] = {}
    for chunk in _chunks(urls):
        sql = (
            f"SELECT {_STATE_COLS} FROM job_state WHERE url IN ({_in_clause(len(chunk))}) "
            "ORDER BY updated_at DESC, seen_key DESC"
        )
        for row in conn.execute(sql, chunk):
            out.setdefault(row["url"], row)
    return out


def _status_since(conn: sqlite3.Connection, seen_keys: Sequence[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for chunk in _chunks(seen_keys):
        sql = (
            "SELECT seen_key, MAX(at) AS at FROM state_events "
            f"WHERE field='status' AND seen_key IN ({_in_clause(len(chunk))}) GROUP BY seen_key"
        )
        for row in conn.execute(sql, chunk):
            out[row["seen_key"]] = row["at"]
    return out


def _load_skills(conn: sqlite3.Connection) -> list[str]:
    row = conn.execute("SELECT value FROM app_settings WHERE key='skills'").fetchone()
    if not row:
        return []
    try:
        val = json.loads(row["value"])
        return [str(s) for s in val] if isinstance(val, list) else []
    except Exception:
        return []


def _comp_band(conn: sqlite3.Connection) -> list[int]:
    row = conn.execute("SELECT value FROM app_settings WHERE key='comp_band'").fetchone()
    if row:
        try:
            band = json.loads(row["value"])
            if isinstance(band, list) and len(band) == 2:
                return [int(band[0]), int(band[1])]
        except Exception:
            pass
    return list(config.DEFAULT_COMP_BAND)


# --------------------------------------------------------------------------- #
# Runs
# --------------------------------------------------------------------------- #
def _completed_runs(conn: sqlite3.Connection) -> list[dict]:
    """Every completed run, oldest first: run_uid, a display run_date (legacy_
    run_date if this is a migrated run, else requested_at), and recorded_at_hint
    (used by _url_as_of for the disappeared-jobs temporal lookup)."""
    ph = ",".join("?" * len(_COMPLETED_RUN_STATUSES))
    rows = conn.execute(
        "SELECT run_uid, COALESCE(legacy_run_date, requested_at) AS run_date, "
        "COALESCE(requested_at, legacy_ingested_at, legacy_run_date) AS recorded_at_hint, "
        "kept_count, new_count "
        f"FROM pipeline_runs WHERE status IN ({ph}) "
        "ORDER BY COALESCE(legacy_run_date, requested_at), run_uid",
        _COMPLETED_RUN_STATUSES,
    ).fetchall()
    return [dict(r) for r in rows]


def _latest_run(conn: sqlite3.Connection) -> dict | None:
    runs = _completed_runs(conn)
    return runs[-1] if runs else None


def _run_membership(conn: sqlite3.Connection, run_uid: str) -> dict[str, str | None]:
    """{posting_id: posting_version_id} for postings actually a MEMBER of this
    run. `membership_kind='snapshot'` excludes migration 11's 'current-only'
    rows -- those assert "this posting's current state as of the backfill", not
    "this posting was observed in this run", and `compat_job_history` excludes
    them for the identical reason (migrations.py). Without the filter, every
    legacy-backfilled posting would appear to have been a member of every
    completed run, which was never observed."""
    return {
        row["posting_id"]: row["posting_version_id"]
        for row in conn.execute(
            "SELECT posting_id, posting_version_id FROM run_postings "
            "WHERE run_uid=? AND present=1 AND membership_kind='snapshot'",
            (run_uid,),
        )
    }


def _tier_for_version(conn: sqlite3.Connection, version_id: str | None) -> int | None:
    if version_id is None:
        return None
    row = conn.execute(
        "SELECT tier FROM score_versions WHERE posting_version_id=? "
        "ORDER BY created_at ASC, score_version_id ASC LIMIT 1",
        (version_id,),
    ).fetchone()
    if row is not None and row["tier"] is not None:
        return row["tier"]
    row2 = conn.execute(
        "SELECT tier FROM posting_versions WHERE posting_version_id=?", (version_id,)
    ).fetchone()
    return row2["tier"] if row2 is not None else None


def _first_seen_flags(conn: sqlite3.Connection, run_uid: str | None, posting_ids: Sequence[str]) -> dict[str, bool]:
    out: dict[str, bool] = {}
    if run_uid is None:
        return out
    for chunk in _chunks(posting_ids):
        sql = (
            "SELECT posting_id, first_seen_in_run FROM run_postings "
            f"WHERE run_uid=? AND posting_id IN ({_in_clause(len(chunk))})"
        )
        for row in conn.execute(sql, [run_uid, *chunk]):
            out[row["posting_id"]] = bool(row["first_seen_in_run"])
    return out


def _present_posting_ids(conn: sqlite3.Connection) -> list[str]:
    return [
        row["posting_id"]
        for row in conn.execute("SELECT posting_id FROM postings WHERE absent_since IS NULL ORDER BY posting_id")
    ]


def _postings_first_seen(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, str | None]:
    """{posting_id: postings.first_seen_at}, the COALESCE fallback for a
    version's NULL `first_seen` -- exactly `compat_jobs`'s rule (migrations.py:
    `COALESCE(v.first_seen, p.first_seen_at)`)."""
    out: dict[str, str | None] = {}
    for chunk in _chunks(posting_ids):
        sql = f"SELECT posting_id, first_seen_at FROM postings WHERE posting_id IN ({_in_clause(len(chunk))})"
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = row["first_seen_at"]
    return out


def _postings_returned_at(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, str | None]:
    """{posting_id: postings.returned_at} -- Phase 2.4's sticky absent-then-
    returned marker `changes()` uses to derive `reposted` (see module
    docstring)."""
    out: dict[str, str | None] = {}
    for chunk in _chunks(posting_ids):
        sql = f"SELECT posting_id, returned_at FROM postings WHERE posting_id IN ({_in_clause(len(chunk))})"
        for row in conn.execute(sql, chunk):
            out[row["posting_id"]] = row["returned_at"]
    return out


# --------------------------------------------------------------------------- #
# The one shared assembly: posting_id -> JobLight-shaped dict
# --------------------------------------------------------------------------- #
def build_light_rows(conn: sqlite3.Connection, posting_ids: Sequence[str]) -> dict[str, dict]:
    """{posting_id: JobLight-dict} for the given posting ids.

    Every dict carries every `JobLight` field (validated through the pydantic
    model, so a genuine type mismatch fails loudly rather than serializing
    silently) PLUS an add-only `posting_id` key -- the spec's example of an
    allowed extra field. A posting with literally no `posting_versions` row
    contributes no entry (see the fallback-chain note in the module docstring).
    """
    ids = list(dict.fromkeys(posting_ids))
    if not ids:
        return {}

    version_by_posting = _version_id_for_postings(conn, ids)
    version_ids = list(version_by_posting.values())
    versions = _version_rows(conn, version_ids)
    scores = _current_scores(conn, version_ids)
    urls = _display_urls(conn, ids)
    descs = _latest_descriptions(conn, ids)
    first_seen_at = _postings_first_seen(conn, ids)
    all_namespaces = _all_namespaces(conn, ids)

    state_by_pid = _state_by_posting(conn, ids)
    fallback_targets = {p: urls[p] for p in ids if p not in state_by_pid and urls.get(p)}
    state_by_url = _state_by_url(conn, list(fallback_targets.values())) if fallback_targets else {}

    seen_keys = {row["seen_key"] for row in state_by_pid.values()}
    seen_keys |= {row["seen_key"] for row in state_by_url.values()}
    status_since = _status_since(conn, sorted(seen_keys))

    latest = _latest_run(conn)
    is_new_map = _first_seen_flags(conn, latest["run_uid"] if latest else None, ids)

    out: dict[str, dict] = {}
    for pid in ids:
        vid = version_by_posting.get(pid)
        version = versions.get(vid) if vid else None
        if version is None:
            continue

        score = scores.get(vid)
        rationale = _rationale(score["rationale_json"]) if score is not None else {}
        if score is not None:
            tier = score["tier"]
            odds = score["odds"]
            odds_score = score["odds_score"]
            why = rationale.get("why")
            flags = _flags_str(rationale.get("flags"))
            odds_why = rationale.get("odds_why")
        else:
            tier = version["tier"]
            odds = version["odds"]
            odds_score = version["odds_score"]
            why = version["why"]
            flags = _flags_str(version["flags"])
            odds_why = version["odds_why"]

        url = urls.get(pid) or ""
        state_row = state_by_pid.get(pid)
        if state_row is None:
            state_row = state_by_url.get(url)

        job_state = None
        if state_row is not None and state_row["status"] is not None:
            job_state = JobState(
                status=state_row["status"],
                notes=state_row["notes"] or "",
                follow_up_date=state_row["follow_up_date"],
                applied_date=state_row["applied_date"],
                starred=bool(state_row["starred"]),
                hidden=bool(state_row["hidden"]),
                contact=state_row["contact"] or "",
                snoozed_until=state_row["snoozed_until"],
                applied_via=state_row["applied_via"],
                updated_at=state_row["updated_at"] or "",
                status_since=status_since.get(state_row["seen_key"]),
            )

        desc = descs.get(pid)
        has_desc = bool(desc is not None and desc["fetch_status"] == "available" and desc["body"])
        desc_snippet = version["desc_snippet"]
        if not desc_snippet and desc is not None and desc["body"]:
            desc_snippet = desc["body"][:500]

        # NULL-column recovery for scheduler-written versions (see module
        # docstring's SALARY / FIRST-SEEN / ALSO-SEEN-ON RECOVERY note).
        # `runstore._link_source_version` never writes salary_min/salary_max/
        # first_seen/also_seen_on, and only writes salary/posted/remote from
        # the record it was handed -- so payload_json's `canonical` fields are
        # the fallback for all but the two salary numbers, which have no
        # structured source anywhere and are parsed from text instead.
        payload_canonical = _payload_canonical(version["payload_json"])

        salary_text = version["salary"]
        if salary_text is None:
            salary_text = payload_canonical.get("salary")

        salary_min, salary_max = version["salary_min"], version["salary_max"]
        if salary_min is None and salary_max is None:
            salary_min, salary_max = _parse_salary_range(salary_text)

        posted = version["posted"]
        if posted is None:
            posted = payload_canonical.get("posted_date") or None

        remote_val = version["remote"]
        if remote_val is None:
            remote_val = 1 if payload_canonical.get("remote") == "1" else 0

        first_seen = version["first_seen"]
        if first_seen is None:
            first_seen = first_seen_at.get(pid)

        also_seen_on = version["also_seen_on"]
        if also_seen_on is None:
            others = sorted(
                ns for ns in all_namespaces.get(pid, ()) if ns and ns != version["source"]
            )
            also_seen_on = ", ".join(others) if others else None

        light = JobLight(
            url=url,
            url_b64=url_to_b64(url),
            seen_key=pid,
            tier=tier if tier is not None else 0,
            odds=odds,
            odds_score=odds_score,
            odds_why=odds_why,
            is_new=is_new_map.get(pid, False),
            title=version["title"],
            company=version["company"],
            location=version["location"],
            salary=salary_text,
            salary_min=salary_min,
            salary_max=salary_max,
            posted=posted,
            first_seen=first_seen,
            remote=bool(remote_val),
            source=version["source"],
            also_seen_on=also_seen_on,
            req_id=version["req_id"],
            why=why,
            flags=flags,
            desc_snippet=desc_snippet,
            has_desc=has_desc,
            state=job_state,
        )
        out[pid] = {**light.model_dump(), "posting_id": pid}
    return out


# --------------------------------------------------------------------------- #
# Public read functions
# --------------------------------------------------------------------------- #
def list_jobs(conn: sqlite3.Connection, *, min_tier: int | None = None) -> dict:
    posting_ids = _present_posting_ids(conn)
    light = build_light_rows(conn, posting_ids)
    jobs = [light[pid] for pid in posting_ids if pid in light]
    if min_tier is not None:
        jobs = [j for j in jobs if j["tier"] >= min_tier]
    latest = _latest_run(conn)
    return {"run_date": latest["run_date"] if latest else None, "jobs": jobs}


def followups(conn: sqlite3.Connection) -> dict:
    posting_ids = _present_posting_ids(conn)
    light = build_light_rows(conn, posting_ids)
    today = today_iso()
    horizon = date_plus(14)

    candidates: list[tuple[str, dict]] = []
    for pid in posting_ids:
        job = light.get(pid)
        if job is None:
            continue
        state = job.get("state")
        if not state:
            continue
        fud = state.get("follow_up_date")
        if fud is None:
            continue
        if state.get("status") not in config.ACTIVE_STATUSES:
            continue
        if state.get("hidden"):
            continue
        candidates.append((fud, job))
    candidates.sort(key=lambda t: t[0])

    overdue = [job for fud, job in candidates if fud < today]
    upcoming = [job for fud, job in candidates if today <= fud <= horizon]
    return {"overdue": overdue, "upcoming": upcoming}


def job_detail(conn: sqlite3.Connection, url: str) -> dict | None:
    pid = _posting_id_for_url(conn, url)
    if pid is None:
        return None
    light = build_light_rows(conn, [pid])
    job = light.get(pid)
    if job is None:
        return None

    desc = _latest_descriptions(conn, [pid]).get(pid)
    full_desc = desc["body"] if desc is not None else None
    haystack = ((full_desc or "") + "\n" + (job.get("desc_snippet") or "")).lower()
    skill_hits: list[str] = []
    for skill in _load_skills(conn):
        s = skill.strip().lower()
        if s and s in haystack and skill not in skill_hits:
            skill_hits.append(skill)

    return {**job, "full_desc": full_desc, "skill_hits": skill_hits}


#: A bare YYYY-MM-DD date, as opposed to a full ISO timestamp or a run_uid --
#: see `_resolve_baseline`.
_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _resolve_baseline(runs: list[dict], since: str | None, current: dict | None) -> dict | None:
    """The baseline run `since` names, or None (falls back to the previous run).

    `since` is accepted in THREE forms, tried in this order:
      1. an exact `run_uid` -- the unambiguous form, and the only one that
         works when two runs share a display date (below).
      2. an exact `run_date` match -- covers a migrated run's YYYY-MM-DD
         `legacy_run_date` and, incidentally, a canonical run's full ISO
         `requested_at` if the caller happens to pass it back verbatim.
      3. a bare YYYY-MM-DD date matched as a PREFIX of `run_date` -- the form
         a caller reasonably passes for a canonical run, whose displayed
         `run_date` is a full ISO timestamp (`requested_at`), never a bare
         date. The NEWEST run that day wins (`runs` is oldest-first).
    `run_date`'s identity rule is otherwise unchanged: full ISO for a
    canonical run, YYYY-MM-DD for a migrated one (see module docstring).
    """
    if since is None:
        return None
    match = next((r for r in runs if r["run_uid"] == since), None)
    if match is None:
        match = next((r for r in runs if r["run_date"] == since), None)
    if match is None and _DATE_ONLY.match(since):
        candidates = [r for r in runs if (r["run_date"] or "").startswith(since)]
        match = candidates[-1] if candidates else None
    if match is not None and current is not None and match["run_uid"] != current["run_uid"]:
        return match
    return None


def changes(conn: sqlite3.Connection, since: str | None = None) -> dict:
    runs = _completed_runs(conn)
    current = runs[-1] if runs else None

    baseline = _resolve_baseline(runs, since, current)
    if baseline is None:
        baseline = runs[-2] if len(runs) >= 2 else None

    empty = {
        "baseline": baseline["run_date"] if baseline else None,
        "current": current["run_date"] if current else None,
        "new": [], "reposted": [], "tier_changed": [], "disappeared": [],
    }
    if current is None or baseline is None:
        return empty

    base_members = _run_membership(conn, baseline["run_uid"])
    curr_members = _run_membership(conn, current["run_uid"])
    base_ids, curr_ids = set(base_members), set(curr_members)

    present_ids = _present_posting_ids(conn)
    light = build_light_rows(conn, present_ids)

    new_jobs = [light[pid] for pid in sorted(curr_ids - base_ids) if pid in light]

    # `reposted`: no canonical equivalent of legacy `cmd_score`'s seen-key
    # ledger (see module docstring). Derived instead from `postings.
    # returned_at` -- a posting counts as reposted in THIS window when it went
    # absent and came back strictly between the baseline and current runs'
    # `recorded_at_hint`s. `returned_at` is sticky (never cleared by a later
    # absence), so the window bound is load-bearing: without it, any posting
    # that ever returned once would read as reposted forever.
    base_hint, curr_hint = baseline["recorded_at_hint"], current["recorded_at_hint"]
    returned_ats = _postings_returned_at(conn, sorted(curr_ids))
    reposted = []
    if base_hint is not None and curr_hint is not None:
        for pid in sorted(curr_ids):
            rat = returned_ats.get(pid)
            if rat and base_hint < rat <= curr_hint:
                job = light.get(pid)
                if job is not None:
                    reposted.append(job)

    tier_changed = []
    for pid in sorted(curr_ids & base_ids):
        b_tier = _tier_for_version(conn, base_members[pid])
        c_tier = _tier_for_version(conn, curr_members[pid])
        if b_tier != c_tier:
            job = light.get(pid)
            if job is not None:
                tier_changed.append({"job": job, "from": b_tier, "to": c_tier})

    disappeared = []
    for pid in sorted(base_ids - curr_ids):
        version_id = base_members[pid]
        version = _version_rows(conn, [version_id]).get(version_id) if version_id else None
        url = _url_as_of(conn, pid, baseline["recorded_at_hint"]) or urls_fallback(conn, pid)
        disappeared.append({
            "url": url or "",
            "url_b64": url_to_b64(url or ""),
            "title": version["title"] if version is not None else None,
            "company": version["company"] if version is not None else None,
            "location": version["location"] if version is not None else None,
            "tier": _tier_for_version(conn, version_id),
            "last_seen": baseline["run_date"],
        })

    return {
        "baseline": baseline["run_date"],
        "current": current["run_date"],
        "new": new_jobs,
        "reposted": reposted,
        "tier_changed": tier_changed,
        "disappeared": disappeared,
    }


def urls_fallback(conn: sqlite3.Connection, posting_id: str) -> str | None:
    """Last resort for a historical url: the posting's current display url, used
    only when no alias was valid at the baseline instant (e.g. an alias record
    with no valid_from information)."""
    return _display_urls(conn, [posting_id]).get(posting_id)


def analytics(conn: sqlite3.Connection) -> dict:
    posting_ids = _present_posting_ids(conn)
    light = build_light_rows(conn, posting_ids)
    jobs = [light[pid] for pid in posting_ids if pid in light]

    funnel: dict[str, int] = {}
    for job in jobs:
        state = job.get("state")
        status = state["status"] if state else "New"
        funnel[status] = funnel.get(status, 0) + 1

    # Dormant advanced-status job_state rows whose linked posting (posting_id
    # first, url fallback) is absent or unresolvable -- present postings are
    # already counted above via `jobs`, mirroring the legacy funnel's second
    # query (`j.url IS NULL OR j.present = 0`).
    #
    # A dedup guard used to live here comparing `row["seen_key"]` (this
    # table's real primary key, e.g. an opaque legacy hash) against
    # `job["seen_key"]`, which for a v2 `JobLight` is actually the POSTING_ID
    # (see module docstring's key-consistency note) -- two key spaces that
    # never collide, so the guard was always a no-op and the posting_id/url
    # resolution below carried the real work by itself, "correct only by
    # accident" in the sense that nothing here actually depended on it. Fixed
    # by removing it rather than re-keying it on posting_id: once keyed
    # correctly it is provably redundant with `pid in present_set` below --
    # every posting counted in `jobs` is, by construction, already in
    # `present_set` -- so a second guard under a different name would only be
    # dead weight again.
    present_set = set(posting_ids)
    ph = ",".join("?" * len(config.ADVANCED_STATUSES))
    alias_cache: dict[str, str | None] = {}
    for row in conn.execute(
        f"SELECT seen_key, url, posting_id, status FROM job_state WHERE status IN ({ph})",
        tuple(config.ADVANCED_STATUSES),
    ):
        pid = row["posting_id"]
        if pid is None and row["url"]:
            if row["url"] not in alias_cache:
                alias_cache[row["url"]] = _posting_id_for_url(conn, row["url"])
            pid = alias_cache[row["url"]]
        if pid is not None and pid in present_set:
            continue
        funnel[row["status"]] = funnel.get(row["status"], 0) + 1

    tiers: dict[str, int] = {}
    for job in jobs:
        key = str(job["tier"])
        tiers[key] = tiers.get(key, 0) + 1

    odds = {o: 0 for o in _COMPETITION}
    matrix = {str(t): {o: 0 for o in _COMPETITION} for t in range(5, 0, -1)}
    for job in jobs:
        comp = _competition_of(job.get("odds"))
        if comp in odds:
            odds[comp] += 1
        tkey = str(job["tier"])
        if tkey in matrix and comp in matrix[tkey]:
            matrix[tkey][comp] += 1

    by_source_counts: dict[str, dict[str, int]] = {}
    for job in jobs:
        source = job.get("source")
        bucket = by_source_counts.setdefault(source, {"kept": 0, "with_desc": 0})
        bucket["kept"] += 1
        if job.get("has_desc"):
            bucket["with_desc"] += 1
    by_source = [
        {"source": source, "kept": counts["kept"], "with_desc": counts["with_desc"]}
        for source, counts in sorted(by_source_counts.items(), key=lambda kv: (-kv[1]["kept"], kv[0] or ""))
    ]

    new_per_run = [
        {"run_date": r["run_date"], "kept": r["kept_count"], "new_this_run": r["new_count"]}
        for r in _completed_runs(conn)
    ]

    bucket_counts: dict[int, int] = {}
    for job in jobs:
        lo_v, hi_v = job.get("salary_min"), job.get("salary_max")
        if lo_v is None and hi_v is None:
            continue
        mid = (lo_v + hi_v) / 2 if (lo_v is not None and hi_v is not None) else (lo_v if lo_v is not None else hi_v)
        bucket = int(mid // 10000) * 10000
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    comp_buckets = [
        {"lo": b, "hi": b + 10000, "count": bucket_counts[b]} for b in sorted(bucket_counts)
    ]

    today = today_iso()
    overdue_ph = ",".join("?" * len(config.ACTIVE_STATUSES))
    overdue = conn.execute(
        "SELECT COUNT(*) AS c FROM job_state WHERE follow_up_date IS NOT NULL AND follow_up_date < ? "
        f"AND status IN ({overdue_ph})",
        (today, *config.ACTIVE_STATUSES),
    ).fetchone()["c"]
    upcoming = conn.execute(
        "SELECT COUNT(*) AS c FROM job_state WHERE follow_up_date IS NOT NULL AND follow_up_date >= ?",
        (today,),
    ).fetchone()["c"]

    return {
        "funnel": funnel,
        "tiers": tiers,
        "odds": odds,
        "matrix": matrix,
        "by_source": by_source,
        "new_per_run": new_per_run,
        "comp": {"buckets": comp_buckets, "band": _comp_band(conn)},
        "followups": {"overdue": overdue, "upcoming": upcoming},
        "statuses": config.STATUSES,
    }


def freshness(conn: sqlite3.Connection) -> dict:
    latest = _latest_run(conn)
    row = None
    if latest is not None:
        row = conn.execute(
            "SELECT legacy_run_date, requested_at, legacy_ingested_at, kept_count, new_count, "
            "aggregate_report_json FROM pipeline_runs WHERE run_uid=?",
            (latest["run_uid"],),
        ).fetchone()

    latest_run_date = ingested_at = None
    kept = new_this_run = None
    zero_row_sources: list = []
    stale_refresh_sources: list = []

    if row is not None:
        latest_run_date = row["legacy_run_date"] or row["requested_at"]
        ingested_at = row["legacy_ingested_at"] or row["requested_at"]
        kept = row["kept_count"]
        new_this_run = row["new_count"]
        if row["aggregate_report_json"]:
            try:
                rep = json.loads(row["aggregate_report_json"])
                zero_row_sources = rep.get("zero_row_sources", []) or []
                stale_refresh_sources = rep.get("stale_refresh_sources", []) or []
            except Exception:
                pass

    # Per-source-instance freshness, from source_runs evidence -- always
    # available regardless of whether a run wrote a source_health_json/
    # aggregate_report_json blob, and mapped into the legacy chip shape
    # (name/rows/refreshed/at) so the existing frontend renders it unchanged.
    instances = runstore.source_instance_freshness(conn)
    rows_by_source = _latest_accepted_counts(conn, [i["source"] for i in instances])
    sources = [
        {
            "name": i["source"],
            "rows": rows_by_source.get(i["source"]),
            "refreshed": not i["stale"],
            "at": i["last_attempt_at"],
            # Add-only: the richer signal behind "refreshed", for a v2 UI that
            # wants it. Never consumed by the legacy chip renderer.
            "stale": i["stale"],
            "consecutive_failed_runs": i["consecutive_failed_runs"],
            "age_seconds": i["age_seconds"],
            "last_attempt_status": i["last_attempt_status"],
            "licenses_absence": i["licenses_absence"],
        }
        for i in instances
    ]

    return {
        "latest_run": latest_run_date,
        "ingested_at": ingested_at,
        "kept": kept,
        "new_this_run": new_this_run,
        "sources": sources,
        "zero_row_sources": zero_row_sources,
        "stale_refresh_sources": stale_refresh_sources,
        "sweep": runner.status(),
    }


def _latest_accepted_counts(conn: sqlite3.Connection, sources: Sequence[str]) -> dict[str, int | None]:
    """The most recent 'fetch' attempt's accepted_count per source, best-effort
    for the freshness chip's `rows` field (source_instance_freshness reports
    attempt SUCCESS, not row counts)."""
    out: dict[str, int | None] = {}
    for chunk in _chunks(sources):
        sql = f"""
            SELECT source, accepted_count FROM (
                SELECT source, accepted_count,
                       ROW_NUMBER() OVER (
                           PARTITION BY source
                           ORDER BY COALESCE(finished_at, started_at, requested_at) DESC, attempt DESC
                       ) AS rn
                  FROM source_runs
                 WHERE step=? AND source IN ({_in_clause(len(chunk))})
            ) WHERE rn = 1
        """
        for row in conn.execute(sql, [runstore.SOURCE_RUN_STEP, *chunk]):
            out[row["source"]] = row["accepted_count"]
    return out
