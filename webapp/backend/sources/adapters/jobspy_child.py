"""The JobSpy child process: `python -m backend.sources.adapters.jobspy_child`.

Runs the blocking half of the `jobspy` adapter in its own process, where it can
hold the GIL for minutes and be killed outright at the deadline without taking
the scheduler's event loop with it.

Protocol (see `jobspy.py` for the encoders — parent and child share them, so the
wire shape is defined exactly once):

  stdin   one JSON task spec: `{"version", "site", "country_indeed",
          "hours_old", "start_index", "queries": [...]}`. Read to EOF.
  stdout  NDJSON, and NOTHING else. One line per record
          (`{"type": "record", "record": <NormalizedPosting.to_json_dict()>}`),
          one progress line per finished query, at most one error line.
  stderr  everything else, including jobspy's own logging. The parent drains it
          continuously and keeps the tail for the error message.
  exit    0 success, 75 transient (retry may help), 78 permanent (it will not).

Three rules this module exists to enforce:

  1. `import jobspy` HAPPENS HERE, LAZILY, AND NOWHERE ELSE. The adapter module
     must import cleanly on a machine without jobspy (and in CI, which never
     installs it), so the import lives inside `_load_scrape_jobs` in the child.
  2. STDOUT IS PROTOCOL. jobspy and pandas print progress to stdout; a stray
     line there would corrupt the stream. The real stdout is duplicated to a
     private handle and `sys.stdout` is repointed at stderr before anything
     else runs, so library chatter cannot reach the parent's parser.
  3. FAILURES ARE CLASSIFIED, NEVER SWALLOWED. `scraper.src_jobspy` printed a
     failed query and carried on, which made a blocked site indistinguishable
     from an empty one (contract invariant 3). Here the first failing query
     ends the child with a transient error line; because the parent has already
     checkpointed the completed queries, the scheduler's single retry resumes
     at the failure instead of redoing the run.
"""
from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, TextIO

from ..contract import Disposition, InventoryScope, SourceTarget
from .jobspy import (
    EXIT_OK,
    EXIT_PERMANENT,
    EXIT_TRANSIENT,
    SOURCE_KEY,
    WIRE_VERSION,
    encode_error_line,
    encode_progress_line,
    encode_record_line,
    normalize_row,
)

__all__ = ["main"]


def protocol_stream() -> TextIO:
    """Take private ownership of stdout, then repoint `sys.stdout` at stderr.

    Returns the handle the protocol is written to. After this call, anything
    that prints (jobspy, pandas, a stray `print`) lands on stderr, where it is
    harmless.
    """
    try:
        fileno = sys.stdout.fileno()
    except (AttributeError, OSError, ValueError):  # pragma: no cover - odd hosts
        return sys.stdout
    sys.stdout.flush()
    stream = os.fdopen(os.dup(fileno), "w", encoding="utf-8", newline="\n")
    sys.stdout = sys.stderr
    return stream


def _emit(stream: TextIO, line: str) -> None:
    """Write one protocol line and flush it.

    Flushing per line is the streaming guarantee (contract invariant 6): a
    buffered child would hand the parent minutes of records at once, and the
    Success Contract's "new jobs reach the UI before the run finishes" would be
    lost for the source that needs it most.
    """
    stream.write(line)
    stream.flush()


def _rows(frame: Any) -> Sequence[Mapping[str, Any]]:
    """`scrape_jobs`'s DataFrame -> plain row dicts.

    Duck-typed so this module never imports pandas itself. `NaN` cells survive
    as floats, which is exactly what `normalize_row` is written to handle.
    """
    if frame is None:
        return ()
    to_dict = getattr(frame, "to_dict", None)
    if callable(to_dict):
        try:
            rows = to_dict(orient="records")
        except TypeError:  # pragma: no cover - not a DataFrame after all
            rows = None
        if isinstance(rows, Sequence):
            return [row for row in rows if isinstance(row, Mapping)]
    if isinstance(frame, Iterable):
        return [row for row in frame if isinstance(row, Mapping)]
    return ()


def _load_scrape_jobs() -> Any:
    """Import jobspy. Deferred to call time on purpose — see rule 1 above."""
    from jobspy import scrape_jobs

    return scrape_jobs


def _target(spec: Mapping[str, Any]) -> SourceTarget:
    """Rebuild just enough target for `normalize_row` to stamp identity.

    Only `source_key` and `instance_key` matter to the records: they are what
    `NormalizedPosting.namespace` is built from, and the parent re-checks them
    on every line it decodes.
    """
    site = str(spec.get("site") or spec.get("instance_key") or "")
    return SourceTarget(
        source_key=SOURCE_KEY,
        instance_key=str(spec.get("instance_key") or site),
        label=f"JobSpy {site}",
        params={"site": site},
        inventory_scope=InventoryScope.PARTIAL,
    )


def _run_query(
    scrape: Any, spec: Mapping[str, Any], query: Mapping[str, Any]
) -> Sequence[Mapping[str, Any]]:
    """One `scrape_jobs` call, with `scraper.src_jobspy`'s exact arguments."""
    frame = scrape(
        site_name=[str(spec.get("site") or "")],
        search_term=str(query.get("term") or ""),
        google_search_term=str(query.get("google_search_term") or ""),
        location=str(query.get("location") or ""),
        is_remote=bool(query.get("is_remote")),
        results_wanted=int(query.get("results_wanted") or 0),
        hours_old=spec.get("hours_old"),
        country_indeed=str(spec.get("country_indeed") or ""),
    )
    return _rows(frame)


def main(argv: Sequence[str] | None = None) -> int:
    """Read the spec, run the queries from `start_index`, stream the records."""
    del argv  # no command-line options: the whole task arrives on stdin
    out = protocol_stream()
    raw_spec = sys.stdin.read()
    try:
        spec = json.loads(raw_spec or "{}")
    except ValueError as exc:
        _emit(out, encode_error_line(Disposition.PERMANENT, f"unparseable task spec: {exc}"))
        return EXIT_PERMANENT
    if not isinstance(spec, Mapping):
        _emit(out, encode_error_line(Disposition.PERMANENT, "task spec is not an object"))
        return EXIT_PERMANENT
    if int(spec.get("version") or 0) != WIRE_VERSION:
        _emit(
            out,
            encode_error_line(
                Disposition.PERMANENT,
                f"unsupported task spec version {spec.get('version')!r}",
            ),
        )
        return EXIT_PERMANENT

    queries = spec.get("queries")
    if not isinstance(queries, Sequence) or isinstance(queries, (str, bytes)):
        _emit(out, encode_error_line(Disposition.PERMANENT, "task spec has no queries list"))
        return EXIT_PERMANENT

    try:
        scrape = _load_scrape_jobs()
    except ImportError as exc:
        # Legacy printed "not installed — skipping" and returned []. Under this
        # contract that is a lie the scheduler would record as a clean, empty
        # run; a missing dependency is permanent and must be visible.
        _emit(out, encode_error_line(Disposition.PERMANENT, f"jobspy is not importable: {exc}"))
        return EXIT_PERMANENT

    target = _target(spec)
    start_index = max(0, int(spec.get("start_index") or 0))
    for index, query in enumerate(queries):
        if index < start_index or not isinstance(query, Mapping):
            continue
        try:
            rows = _run_query(scrape, spec, query)
        except Exception as exc:  # noqa: BLE001 - classified, then re-raised as an exit code
            _emit(
                out,
                encode_error_line(
                    Disposition.TRANSIENT,
                    f"query {index} ({query.get('term')!r} @ {query.get('location')!r}) "
                    f"failed: {type(exc).__name__}: {exc}",
                ),
            )
            return EXIT_TRANSIENT
        count = 0
        for row in rows:
            record = normalize_row(row, target, query=query)
            if record is None:
                # No title or no URL: unopenable and unidentifiable. One bad row
                # must not fail the query.
                continue
            _emit(out, encode_record_line(record))
            count += 1
        _emit(out, encode_progress_line(index, count=count))
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    try:
        raise SystemExit(main(sys.argv[1:]))
    except SystemExit:
        raise
    except BaseException as exc:  # noqa: BLE001 - last resort, must stay classified
        sys.stderr.write(f"jobspy child crashed: {type(exc).__name__}: {exc}\n")
        raise SystemExit(EXIT_PERMANENT) from exc
