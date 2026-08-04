#!/usr/bin/env python3
"""A frozen stand-in for `jobspy_child`, for `fetch()` tests.

Speaks the same NDJSON protocol without importing jobspy, pandas, `backend`, or
touching the network — the adapter's subprocess plumbing (streaming, checkpoint
progress, cancellation, exit-code classification) is what the tests exercise,
and it must be exercisable on a machine that has none of those.

Deliberately dependency-free and stdlib-only so it can be run by any
interpreter the test picks. Invoked as:

    python fake_child.py stream  <ndjson> [spec_out]   emit lines, exit 0
    python fake_child.py hang    <ndjson>              emit lines, then block
    python fake_child.py fail    <exit_code>           error line, then exit
    python fake_child.py crash   <exit_code>           exit, no error line
    python fake_child.py garbage                       one record, then junk

`stream` and `hang` honour `start_index` from the task spec exactly as the real
child does: a line belongs to query group `g`, where `g` counts the progress
lines already emitted, and groups before `start_index` are skipped.
"""
import json
import sys
import time

EXIT_TRANSIENT = 75
EXIT_PERMANENT = 78


def _emit(line):
    sys.stdout.write(line if line.endswith("\n") else line + "\n")
    sys.stdout.flush()


def _read_spec():
    raw = sys.stdin.read()
    try:
        return json.loads(raw or "{}")
    except ValueError:
        return {}


def _replay(path, start_index):
    """Emit the frozen NDJSON, skipping query groups before `start_index`."""
    with open(path, encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]
    group = 0
    for line in lines:
        payload = json.loads(line)
        if payload.get("type") == "progress":
            index = int(payload.get("query_index"))
            if index >= start_index:
                _emit(line)
            group = index + 1
            continue
        if group >= start_index:
            _emit(line)


def main(argv):
    mode = argv[0] if argv else "stream"
    spec = _read_spec()
    start_index = int(spec.get("start_index") or 0)

    if mode in ("stream", "hang"):
        if len(argv) > 2:
            with open(argv[2], "w", encoding="utf-8") as handle:
                json.dump(spec, handle)
        _replay(argv[1], start_index)
        if mode == "hang":
            while True:  # killed by the parent; never exits on its own
                time.sleep(3600)
        return 0

    if mode == "fail":
        code = int(argv[1])
        disposition = "permanent" if code == EXIT_PERMANENT else "transient"
        _emit(json.dumps({"type": "error", "disposition": disposition, "message": "fake child failure"}))
        return code

    if mode == "crash":
        sys.stderr.write("fake child died without saying anything\n")
        return int(argv[1])

    if mode == "garbage":
        _emit(json.dumps({"type": "progress", "query_index": 0, "count": 0}))
        _emit("Scraping indeed: 42%|####  | 42/100")
        return 0

    sys.stderr.write("unknown fake child mode %r\n" % (mode,))
    return EXIT_PERMANENT


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
