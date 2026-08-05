"""Shared helpers for the task-4.5 validation harness: sandbox-path guarding,
JSON-safe comparison, and the report/log writer.

Nothing here imports `backend.*` -- that import only becomes safe after the
safety gate in `__main__.py` has run, and this module is imported before it.
"""
from __future__ import annotations

import asyncio
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


class SandboxViolation(RuntimeError):
    """A path the harness was about to touch resolves outside --sandbox."""


def require_within(path, sandbox: Path, label: str) -> Path:
    """Resolve `path` and assert it is inside `sandbox`, or raise loudly.

    Every database file this harness opens, creates, copies, or migrates goes
    through this check first -- it is the enforcement mechanism behind the
    module docstring's safety guarantee, not just the one at startup."""
    resolved = Path(path).resolve()
    sandbox_resolved = Path(sandbox).resolve()
    try:
        resolved.relative_to(sandbox_resolved)
    except ValueError:
        raise SandboxViolation(
            f"refusing to touch {label} path {resolved}: it is outside sandbox {sandbox_resolved}"
        ) from None
    return resolved


def copy_into_sandbox(src, dest, sandbox: Path, *, label: str) -> Path:
    """Copy `src` to `dest`, both required to resolve inside `sandbox`.

    `src` is only ever read here (this is how the harness "opens the snapshot
    read-only at most" -- it copies bytes, it never opens the snapshot file
    itself with a writable sqlite3 connection). Copies any `-wal`/`-shm`
    sidecar files alongside it too, defensively."""
    src_resolved = require_within(src, sandbox, f"{label} source")
    dest_resolved = require_within(dest, sandbox, f"{label} destination")
    if not src_resolved.is_file():
        raise FileNotFoundError(f"{label} source does not exist: {src_resolved}")
    if dest_resolved.exists():
        dest_resolved.unlink()
    shutil.copy2(src_resolved, dest_resolved)
    for suffix in ("-wal", "-shm"):
        side = Path(str(src_resolved) + suffix)
        if side.exists():
            shutil.copy2(side, Path(str(dest_resolved) + suffix))
    return dest_resolved


def jsonable(value):
    """Recursively `model_dump()` any pydantic models inside a plain dict/list
    so a legacy router's direct return value compares equal, key for key, to
    a canonical read function's plain-dict return value. Mirrors the helper
    of the same name in tests/test_read_flag.py."""
    if hasattr(value, "model_dump"):
        return jsonable(value.model_dump())
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    return value


#: `JobLight.seen_key` is documented (canonical_reads.py's own module
#: docstring, "CROSS-ENDPOINT KEY CONSISTENCY") as repurposed to carry
#: `posting_id` on the canonical side, never the legacy `job_state.seen_key`
#: string -- a permanent, by-design value substitution, not a parity bug.
#: Every comparison in phase 3 tolerates a value mismatch on this one key
#: (the key must still be PRESENT on both sides -- only its value is exempt).
KNOWN_VALUE_SUBSTITUTIONS = frozenset({"seen_key"})


def _has_url(x) -> bool:
    return isinstance(x, dict) and "url" in x


def _nested_url_field(x) -> str | None:
    """One level deep: the first (sorted, for determinism) key of `x` whose
    OWN value is a dict carrying `url` -- e.g. `changes.tier_changed`'s
    `{"job": {..., "url": ...}, "from": ..., "to": ...}`. Returns the field
    name, or None if `x` is not a dict or has no such field (N10)."""
    if not isinstance(x, dict):
        return None
    for k in sorted(x):
        v = x[k]
        if isinstance(v, dict) and "url" in v:
            return k
    return None


def _element_key(x, nested_field: str | None):
    if not isinstance(x, dict):
        return None
    if nested_field is not None:
        nested = x.get(nested_field)
        return nested.get("url") if isinstance(nested, dict) else None
    return x.get("url")


def compare_json(legacy, canonical, *, allowed_extra=frozenset(),
                  tolerate_value_diff=KNOWN_VALUE_SUBSTITUTIONS, max_samples=5):
    """Structural diff between a legacy response and its canonical counterpart.

    The canonical side may carry extra dict keys named in `allowed_extra`
    (recursively, at any depth) -- the spec's documented add-only fields
    (`posting_id`, freshness's chip extras). Keys in `tolerate_value_diff`
    must be present on both sides; for `seen_key` specifically, the VALUE is
    not skipped outright but checked against the documented repurposing
    (canonical `seen_key` == canonical `posting_id`, see
    canonical_reads.py's "CROSS-ENDPOINT KEY CONSISTENCY" docstring) whenever
    `posting_id` is a sibling key on the canonical side -- a violation counts
    as a diff (N5). Any other discrepancy -- a missing key, an unexpected
    extra key, a value mismatch, a list whose length differs, a row present
    on only one side -- is recorded. Returns
        {"equal": bool, "diff_count": int, "samples": [...],
         "legacy_len": int | None, "canonical_len": int | None,
         "missing_in_canonical_count": int, "extra_in_canonical_count": int,
         "list_lens": [{"path", "legacy_len", "canonical_len"}, ...]}
    `samples` holds at most `max_samples` diff records verbatim (path, kind,
    legacy value, canonical value) even when diff_count is larger, so the
    report stays a fixed size -- but `missing_in_canonical_count` /
    `extra_in_canonical_count` are exact counts over the FULL diff set (B4),
    not just what made it into `samples`, so a caller can see row-loss
    severity independently of the sample cap or of `diff_count` (which mixes
    in value_mismatch/length_mismatch too). `legacy_len`/`canonical_len` are
    the root's own length when the root passed in is itself a list;
    `list_lens` records EVERY list-vs-list pair compared anywhere in the
    tree, at any depth, keyed by path.
    """
    diffs: list[dict] = []
    list_lens: list[dict] = []

    def walk(a, b, path):
        if isinstance(a, dict) and isinstance(b, dict):
            for k in sorted(set(a) - set(b)):
                diffs.append({"path": f"{path}.{k}", "kind": "missing_in_canonical",
                              "legacy": a[k], "canonical": None})
            for k in sorted(set(b) - set(a)):
                if k not in allowed_extra:
                    diffs.append({"path": f"{path}.{k}", "kind": "unexpected_extra_in_canonical",
                                  "legacy": None, "canonical": b[k]})
            for k in sorted(set(a) & set(b)):
                if k in tolerate_value_diff:
                    if k == "seen_key" and "posting_id" in b:
                        expected = b["posting_id"]
                        if b[k] != expected:
                            diffs.append({
                                "path": f"{path}.{k}", "kind": "seen_key_not_posting_id",
                                "legacy": a[k],
                                "canonical": {"seen_key": b[k], "posting_id": expected},
                            })
                    continue
                walk(a[k], b[k], f"{path}.{k}")
        elif isinstance(a, list) and isinstance(b, list):
            list_lens.append({"path": path, "legacy_len": len(a), "canonical_len": len(b)})
            if len(a) != len(b):
                diffs.append({"path": path, "kind": "length_mismatch",
                              "legacy": len(a), "canonical": len(b)})
            # Lists of job-shaped dicts (jobs/followups/changes all return
            # these) are not guaranteed to share an ordering between the
            # legacy SQL path and the canonical posting-table path -- a
            # positional zip would compare unrelated jobs and manufacture
            # spurious diffs. Align by "url" (present on every JobLight-
            # shaped dict), or, failing that, by a field ONE LEVEL DEEP whose
            # own value carries "url" (N10 -- `changes.tier_changed`'s
            # `{"job": {...}}` elements). `all(...)` over an EMPTY list is
            # vacuously True, so an empty side no longer defeats alignment --
            # that vacuous-truthiness gap used to collapse a legacy-N/
            # canonical-0 list into a single length_mismatch diff instead of
            # N missing-row diffs (this was half of the "10 lost reposted
            # rows ... scored one length_mismatch" bug).
            a_key = b_key = None
            if all(_has_url(x) for x in a) and all(_has_url(y) for y in b):
                a_key = {x["url"]: x for x in a}
                b_key = {y["url"]: y for y in b}
            else:
                nested_field = next(
                    (f for f in (_nested_url_field(x) for x in a) if f is not None), None
                ) or next(
                    (f for f in (_nested_url_field(y) for y in b) if f is not None), None
                )
                if nested_field is not None:
                    a_keys = [_element_key(x, nested_field) for x in a]
                    b_keys = [_element_key(y, nested_field) for y in b]
                    if all(k is not None for k in a_keys) and all(k is not None for k in b_keys):
                        a_key = dict(zip(a_keys, a))
                        b_key = dict(zip(b_keys, b))

            if a_key is not None:
                for key in sorted(set(a_key) | set(b_key)):
                    if key not in b_key:
                        diffs.append({"path": f"{path}[url={key}]", "kind": "missing_in_canonical",
                                      "legacy": a_key[key], "canonical": None})
                    elif key not in a_key:
                        diffs.append({"path": f"{path}[url={key}]",
                                      "kind": "unexpected_extra_in_canonical",
                                      "legacy": None, "canonical": b_key[key]})
                    else:
                        walk(a_key[key], b_key[key], f"{path}[url={key}]")
            else:
                # No usable alignment key anywhere in this list (e.g.
                # freshness's "sources" chips, keyed by "name" rather than
                # "url"). Compare the overlapping prefix positionally, but --
                # unlike a bare `zip`, which silently drops the rest -- give
                # every leftover element on the longer side its own diff
                # instead of letting it vanish into the single length_mismatch
                # diff above (the other half of the "21 lost freshness chips"
                # bug: B4).
                overlap = min(len(a), len(b))
                for i in range(overlap):
                    walk(a[i], b[i], f"{path}[{i}]")
                for i in range(overlap, len(a)):
                    diffs.append({"path": f"{path}[{i}]", "kind": "missing_in_canonical",
                                  "legacy": a[i], "canonical": None})
                for i in range(overlap, len(b)):
                    diffs.append({"path": f"{path}[{i}]", "kind": "unexpected_extra_in_canonical",
                                  "legacy": None, "canonical": b[i]})
        else:
            if a != b:
                diffs.append({"path": path, "kind": "value_mismatch", "legacy": a, "canonical": b})

    root_legacy = jsonable(legacy)
    root_canonical = jsonable(canonical)
    walk(root_legacy, root_canonical, "$")
    missing_in_canonical_count = sum(1 for d in diffs if d["kind"] == "missing_in_canonical")
    extra_in_canonical_count = sum(1 for d in diffs if d["kind"] == "unexpected_extra_in_canonical")
    return {
        "equal": len(diffs) == 0,
        "diff_count": len(diffs),
        "samples": diffs[:max_samples],
        "legacy_len": len(root_legacy) if isinstance(root_legacy, list) else None,
        "canonical_len": len(root_canonical) if isinstance(root_canonical, list) else None,
        "missing_in_canonical_count": missing_in_canonical_count,
        "extra_in_canonical_count": extra_in_canonical_count,
        "list_lens": list_lens,
    }


def dump_table(conn, table: str, order_by: str) -> list[dict]:
    """Every row of `table`, as plain dicts, in a stable order."""
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY {order_by}").fetchall()
    return [dict(r) for r in rows]


def row_accounting(pre_rows: list[dict], post_rows: list[dict], pk: str, *, max_samples=5) -> dict:
    """Full-row accounting between a pre- and post-migration dump of the same
    table, keyed by `pk`. `missing_in_post` is the zero-silent-loss signal:
    a non-empty list here is a critical defect. Value diffs are computed only
    over columns present on BOTH sides, so an additive nullable column added
    by a later migration is not itself a false positive."""
    pre_by_pk = {r[pk]: r for r in pre_rows}
    post_by_pk = {r[pk]: r for r in post_rows}
    missing_in_post = sorted(set(pre_by_pk) - set(post_by_pk))
    added_in_post = sorted(set(post_by_pk) - set(pre_by_pk))
    value_diffs = []
    for k in sorted(set(pre_by_pk) & set(post_by_pk)):
        a, b = pre_by_pk[k], post_by_pk[k]
        changed = {c: [a[c], b.get(c)] for c in a if c in b and a[c] != b.get(c)}
        if changed:
            value_diffs.append({"pk": k, "diff": changed})
    return {
        "pre_count": len(pre_rows),
        "post_count": len(post_rows),
        "missing_in_post": missing_in_post,
        "added_in_post": added_in_post,
        "value_diff_count": len(value_diffs),
        "value_diff_samples": value_diffs[:max_samples],
    }


def run_async(coro, timeout: float = 60.0):
    """Run one coroutine to completion with a hard ceiling, mirroring the
    `run()` helper every scheduler/runservice test module defines -- a hang
    in the harness fails loudly instead of wedging a mechanical runner."""

    async def _guarded():
        return await asyncio.wait_for(coro, timeout)

    return asyncio.run(_guarded())


class Report:
    """Accumulates the JSON report and a parallel human-readable log, and
    writes both to disk. Every `log()` line is also echoed to stdout as it
    happens, so a runner that only captures stdout still gets the narrative."""

    def __init__(self, out_path: Path):
        self.out_path = Path(out_path)
        self.log_path = self.out_path.with_suffix(".log") if self.out_path.suffix else \
            Path(str(self.out_path) + ".log")
        self._log_lines: list[str] = []
        self.phases: dict = {}
        self.verdict_inputs: list[dict] = []
        self.environment: dict = {}

    def log(self, msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
        print(line, file=sys.stdout, flush=True)
        self._log_lines.append(line)

    def check(self, name: str, passed: bool, detail: str = "", *, informational: bool = False) -> None:
        """Record one verdict input. `informational=True` (N8) marks a
        measurement-only entry -- a literal-True timing/duration record, the
        safety-gate facts, or an assertion that is structurally inert on this
        particular corpus (e.g. the phase5a absence check when the corpus has
        no source_req-owned postings to test against) -- so the DONE tally
        can separate "asserted and passed/failed" from "recorded for the
        record". An informational entry can still carry `passed=False` (e.g.
        a loud NOT-APPLICABLE) without being counted as a blocking failure."""
        self.verdict_inputs.append({
            "check": name, "pass": bool(passed), "detail": str(detail),
            "informational": bool(informational),
        })
        tag = "PASS" if passed else "FAIL"
        if informational:
            tag += " (informational)"
        self.log(f"CHECK {name}: {tag} -- {detail}")

    def phase(self, name: str, data: dict) -> None:
        self.phases[name] = data

    def write(self) -> None:
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "phases": self.phases,
            "environment": self.environment,
            "verdict_inputs": self.verdict_inputs,
        }
        self.out_path.write_text(json.dumps(payload, indent=2, default=str, sort_keys=True))
        self.log_path.write_text("\n".join(self._log_lines) + "\n")
