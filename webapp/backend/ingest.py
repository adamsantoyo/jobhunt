"""CSV/report -> SQLite ingest with first-run backfill, streaming description join,
and (the critical part) non-destructive job_state healing across url rewrites.

One public function: ingest(conn) -> IngestReport. All data mutations happen in a
single transaction (committed once at the end, rolled back on error). The jobs /
runs / job_history tables are a rebuildable cache; job_state is user-owned and this
function is structurally incapable of clearing a status, note, or date.
"""
import csv
import json
import re
from datetime import date, datetime
from pathlib import Path

from . import config
from .db import init_db
from .descriptions import stream_descriptions
from .events import record_field_events
from .identity import seen_key as compute_seen_key
from .models import IngestReport

_DATE_RE = re.compile(r"jobs_scored_(\d{4}-\d{2}-\d{2})\.csv$")


def _today() -> str:
    return date.today().isoformat()


def _now() -> str:
    return datetime.now().isoformat()


def _int(x):
    """Defensive int parse: '' / None / junk -> None; '130000' / '130000.0' -> 130000."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _run_date_of(path: Path):
    m = _DATE_RE.search(path.name)
    return m.group(1) if m else path.stem


def _read_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _read_csv_rows(path: Path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _surrogate_url(row_url, seen_key):
    """CSV urls are all http(s) today, but defend: empty/non-http -> stable surrogate PK."""
    u = (row_url or "").strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "withheld:" + seen_key


def _new_count(rows):
    return sum(1 for r in rows if (r.get("new") or "").strip().upper() == "NEW")


def _free_orphan_url(cur, seen_key):
    """A free job_state PK to park a detached state row on: 'orphaned:<seen_key>',
    suffixed '#2', '#3', ... if taken (double-recycle, or a prior ingest already
    parked one there). Never collides with live urls (http(s):/withheld: only)."""
    base = "orphaned:" + seen_key
    url, n = base, 1
    while cur.execute("SELECT 1 FROM job_state WHERE url=?", (url,)).fetchone():
        n += 1
        url = f"{base}#{n}"
    return url


def ingest(conn) -> IngestReport:
    init_db(conn)
    results = config.RESULTS
    root = config.ROOT
    cur = conn.cursor()

    try:
        # 1. Discover runs; latest CSV is the current run.
        csvs = sorted(results.glob("jobs_scored_*.csv"))
        if not csvs:
            conn.commit()
            return IngestReport(rows=0, new=0, healed=0, needs_review=0, descs_joined=0, runs_backfilled=0)

        latest_csv = csvs[-1]
        run_date = _run_date_of(latest_csv)

        report = _read_json(results / "run_report.json")
        report_date = (report or {}).get("date")
        source_health = _read_json(results / "source_health.json")
        source_health_json = json.dumps(source_health) if source_health is not None else None

        # 2. First-run backfill: if runs is empty, seed runs + job_history for EVERY csv.
        runs_backfilled = 0
        cur.execute("SELECT COUNT(*) AS c FROM runs")
        if cur.fetchone()["c"] == 0:
            for path in csvs:
                rdate = _run_date_of(path)
                rows = _read_csv_rows(path)
                rep_json = json.dumps(report) if (report is not None and report_date == rdate) else None
                sh_json = source_health_json if report_date == rdate else None
                cur.execute(
                    "INSERT OR REPLACE INTO runs (run_date, kept, new_this_run, report_json, source_health_json, ingested_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (rdate, len(rows), _new_count(rows), rep_json, sh_json, _now()),
                )
                cur.execute("DELETE FROM job_history WHERE run_date=?", (rdate,))
                for r in rows:
                    sk = compute_seen_key(r.get("company"), r.get("title"), r.get("location"))
                    url = _surrogate_url(r.get("url"), sk)
                    cur.execute(
                        "INSERT OR REPLACE INTO job_history (url, run_date, seen_key, tier, odds, present) "
                        "VALUES (?,?,?,?,?,1)",
                        (url, rdate, sk, _int(r.get("tier")) or 0, r.get("odds") or None),
                    )
                runs_backfilled += 1

        # Load latest rows once (reused by steps 3, 4, 6).
        latest_rows = _read_csv_rows(latest_csv)

        # 3. Upsert runs row for the latest run.
        rep_json = json.dumps(report) if (report is not None and report_date == run_date) else None
        cur.execute(
            "INSERT INTO runs (run_date, kept, new_this_run, report_json, source_health_json, ingested_at) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(run_date) DO UPDATE SET kept=excluded.kept, new_this_run=excluded.new_this_run, "
            "report_json=excluded.report_json, source_health_json=excluded.source_health_json, ingested_at=excluded.ingested_at",
            (run_date, len(latest_rows), _new_count(latest_rows), rep_json, source_health_json, _now()),
        )

        # 4. Mark all jobs absent, then upsert the present run (full_desc preserved).
        cur.execute("UPDATE jobs SET present=0")
        present_urls = set()
        seen_by_key = {}   # seen_key -> list[url]
        url_seen_key = {}  # url -> seen_key (identity check for still-present urls)
        for r in latest_rows:
            sk = compute_seen_key(r.get("company"), r.get("title"), r.get("location"))
            url = _surrogate_url(r.get("url"), sk)
            present_urls.add(url)
            seen_by_key.setdefault(sk, []).append(url)
            url_seen_key[url] = sk
            remote = 1 if (r.get("remote") or "").strip().lower() == "true" else 0
            is_new = 1 if (r.get("new") or "").strip().upper() == "NEW" else 0
            cur.execute(
                "INSERT INTO jobs (url, seen_key, tier, odds, odds_score, odds_why, is_new, title, company, "
                "location, salary, salary_min, salary_max, posted, first_seen, remote, source, also_seen_on, "
                "req_id, why, flags, desc_snippet, latest_run, present) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1) "
                "ON CONFLICT(url) DO UPDATE SET seen_key=excluded.seen_key, tier=excluded.tier, odds=excluded.odds, "
                "odds_score=excluded.odds_score, odds_why=excluded.odds_why, is_new=excluded.is_new, "
                "title=excluded.title, company=excluded.company, location=excluded.location, salary=excluded.salary, "
                "salary_min=excluded.salary_min, salary_max=excluded.salary_max, posted=excluded.posted, "
                "first_seen=excluded.first_seen, remote=excluded.remote, source=excluded.source, "
                "also_seen_on=excluded.also_seen_on, req_id=excluded.req_id, why=excluded.why, flags=excluded.flags, "
                "desc_snippet=excluded.desc_snippet, latest_run=excluded.latest_run, present=1",
                (
                    url, sk, _int(r.get("tier")) or 0, r.get("odds") or None, _int(r.get("odds_score")),
                    r.get("odds_why") or None, is_new, r.get("title") or None, r.get("company") or None,
                    r.get("location") or None, r.get("salary") or None, _int(r.get("salary_min")),
                    _int(r.get("salary_max")), r.get("posted") or None, r.get("first_seen") or None, remote,
                    r.get("source") or None, r.get("also_seen_on") or None, r.get("req_id") or None,
                    r.get("why") or None, r.get("flags") or None, r.get("desc_snippet") or None, run_date,
                ),
            )

        # 5. Streaming description join: only present rows missing a full_desc.
        cur.execute("SELECT url FROM jobs WHERE present=1 AND full_desc IS NULL")
        wanted = {row["url"] for row in cur.fetchall()}
        descs = stream_descriptions(wanted, results_dir=results)
        for url, desc in descs.items():
            cur.execute("UPDATE jobs SET full_desc=? WHERE url=?", (desc, url))
        descs_joined = len(descs)

        # 6. job_history for the latest run: replace the run's snapshot wholesale so a
        # same-day re-ingest (rewritten CSV) never leaves stale rows marked present.
        cur.execute("DELETE FROM job_history WHERE run_date=?", (run_date,))
        for r in latest_rows:
            sk = compute_seen_key(r.get("company"), r.get("title"), r.get("location"))
            url = _surrogate_url(r.get("url"), sk)
            cur.execute(
                "INSERT OR REPLACE INTO job_history (url, run_date, seen_key, tier, odds, present) "
                "VALUES (?,?,?,?,?,1)",
                (url, run_date, sk, _int(r.get("tier")) or 0, r.get("odds") or None),
            )

        # 7. Heal job_state (never destructive). Two passes:
        #    A) identity-check rows whose url is still present — a seen_key mismatch
        #       means the ATS recycled the url for a different role, so the state is
        #       detached onto a surrogate url (never inherited by the new role);
        #    B) seen-key heal for every unanchored row (originally absent + detached).
        # present_with_state is recomputed between the passes so Pass B decisions
        # never depend on state_rows iteration order.
        cur.execute("SELECT url, seen_key, needs_review, review_dismissed FROM job_state")
        state_rows = cur.fetchall()

        healed = 0
        needs_review = 0
        to_heal = []  # (url, seen_key, was_flagged, review_dismissed)

        # Pass A: identity check for still-present urls.
        for srow in state_rows:
            old_url = srow["url"]
            if old_url not in present_urls:
                to_heal.append((old_url, srow["seen_key"], srow["needs_review"], srow["review_dismissed"]))
                continue
            if srow["seen_key"] == url_seen_key.get(old_url):
                # Anchored to the same role; any lingering review state is resolved.
                if srow["needs_review"] or srow["review_dismissed"]:
                    cur.execute(
                        "UPDATE job_state SET needs_review=0, review_reason=NULL, review_dismissed=0, updated_at=? WHERE url=?",
                        (_now(), old_url),
                    )
                continue
            # url recycled to a different role: park the state on a surrogate so the
            # new role shows no inherited status/notes, then heal it in Pass B.
            surrogate = _free_orphan_url(cur, srow["seen_key"])
            cur.execute(
                "UPDATE job_state SET url=?, review_reason=?, updated_at=? WHERE url=?",
                (surrogate, f"url recycled to a different role (was {old_url})"[:2000], _now(), old_url),
            )
            to_heal.append((surrogate, srow["seen_key"], srow["needs_review"], srow["review_dismissed"]))

        # Which present urls carry a state row, post-detach.
        cur.execute("SELECT url FROM job_state")
        present_with_state = {row["url"] for row in cur.fetchall() if row["url"] in present_urls}

        # Pass B: seen-key heal for unanchored rows.
        for old_url, sk, was_flagged, dismissed in to_heal:
            cands = seen_by_key.get(sk, [])
            if len(cands) == 1 and cands[0] not in present_with_state:
                new_url = cands[0]
                cur.execute(
                    "UPDATE job_state SET url=?, seen_key=?, needs_review=0, review_reason=NULL, "
                    "review_dismissed=0, updated_at=? WHERE url=?",
                    (new_url, sk, _now(), old_url),
                )
                present_with_state.add(new_url)
                healed += 1
            elif len(cands) == 0:
                # Job disappeared entirely; keep state dormant, do NOT flag as review.
                # (A detached row keeps its forensic review_reason from Pass A.)
                if was_flagged:
                    cur.execute(
                        "UPDATE job_state SET needs_review=0, review_reason=NULL, updated_at=? WHERE url=?",
                        (_now(), old_url),
                    )
            else:
                # Ambiguous (>=2 candidates, or the sole candidate already has state).
                reason = "ambiguous url rewrite; candidates: " + ", ".join(cands)
                if dismissed:
                    # User already acknowledged this ambiguity: keep the reason fresh
                    # but do not re-flag (durable dismiss).
                    cur.execute(
                        "UPDATE job_state SET review_reason=? WHERE url=?",
                        (reason[:2000], old_url),
                    )
                else:
                    cur.execute(
                        "UPDATE job_state SET needs_review=1, review_reason=?, updated_at=? WHERE url=?",
                        (reason[:2000], _now(), old_url),
                    )
                    needs_review += 1

        # 8. Picks seeding: only where the url is present and has no state yet.
        cur.execute("SELECT url FROM job_state")
        have_state = {row["url"] for row in cur.fetchall()}
        for pick_file in ("picks.json", "picks_llm.json"):
            picks = _read_json(root / pick_file)
            if not isinstance(picks, list):
                continue
            for p in picks:
                if not isinstance(p, dict):
                    continue
                purl = (p.get("url") or "").strip()
                if not purl or purl not in present_urls or purl in have_state:
                    continue
                cur.execute("SELECT seen_key FROM jobs WHERE url=?", (purl,))
                jr = cur.fetchone()
                if jr is None:
                    continue
                reason = (p.get("reason") or "").strip()
                notes = ("[pick] " + reason) if reason else "[pick]"
                seeded_at = _now()
                cur.execute(
                    "INSERT INTO job_state (url, seen_key, status, notes, starred, updated_at) "
                    "VALUES (?,?,?,?,1,?)",
                    (purl, jr["seen_key"], "Interested", notes, seeded_at),
                )
                # Record the seeded fields as events (new row -> old is all-NULL).
                record_field_events(
                    cur, seen_key=jr["seen_key"], url=purl, old={},
                    new={"status": "Interested", "notes": notes, "starred": 1},
                    source="ingest:picks", at=seeded_at,
                )
                have_state.add(purl)

        # 9. Seed app_settings once (INSERT OR IGNORE — never overwrite user edits).
        cfg = _read_json(root / "config.json") or {}
        search_terms = (cfg.get("profile") or {}).get("search_terms")
        if not isinstance(search_terms, list) or not search_terms:
            search_terms = list(config.DEFAULT_SKILLS)
        cur.execute(
            "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)",
            ("skills", json.dumps(search_terms)),
        )
        cur.execute(
            "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)",
            ("comp_band", json.dumps(list(config.DEFAULT_COMP_BAND))),
        )

        conn.commit()
        return IngestReport(
            rows=len(latest_rows),
            new=_new_count(latest_rows),
            healed=healed,
            needs_review=needs_review,
            descs_joined=descs_joined,
            runs_backfilled=runs_backfilled,
        )
    except Exception:
        conn.rollback()
        raise
