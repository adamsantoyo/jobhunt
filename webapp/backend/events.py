"""Append-only audit of job_state field changes (the state_events log).

Every mutation of a user-owned state field records one row per *actually-changed*
field, so downstream views (the application funnel, per-job history) can be built
from an event stream instead of inferred from mutable current state. Writes happen
inside the caller's transaction (no commit here): the event and the state change it
describes must commit atomically, or neither does.
"""
import sqlite3

# Fields whose changes are worth an event. The retired review_* columns and the
# write-only review_dismissed flag are deliberately excluded (internal bookkeeping,
# and being removed entirely in a later phase).
TRACKED_FIELDS = (
    "status", "notes", "follow_up_date", "applied_date",
    "starred", "hidden", "contact", "snoozed_until", "applied_via",
)


def _norm(field: str, value) -> str | None:
    """Serialize a state value to the event's TEXT column. Booleans/ints for the
    flag columns collapse to '0'/'1'; None and '' both normalize to NULL so an empty
    note and an absent note never read as a change; everything else is str()."""
    if value is None:
        return None
    if field in ("starred", "hidden"):
        return "1" if value in (1, True, "1") else "0"
    s = str(value)
    return s if s != "" else None


def record_field_events(conn: sqlite3.Connection, *, seen_key: str, url,
                        old: dict, new: dict, source: str, at: str) -> int:
    """Insert one state_event per tracked field in `new` whose normalized value
    differs from `old` (a missing key in `old` == NULL). `old`/`new` are field->value
    dicts. Returns the count written. Caller owns the transaction and the commit."""
    n = 0
    for field, raw_new in new.items():
        if field not in TRACKED_FIELDS:
            continue
        ov = _norm(field, old.get(field))
        nv = _norm(field, raw_new)
        if ov == nv:
            continue
        conn.execute(
            "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source) "
            "VALUES (?,?,?,?,?,?,?)",
            (seen_key or "", url, field, ov, nv, at, source),
        )
        n += 1
    return n
