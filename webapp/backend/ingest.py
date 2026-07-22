"""CSV/report -> SQLite ingest with first-run backfill and streaming description join.

One public function: ingest(conn) -> IngestReport. All data mutations happen in a
single transaction (committed once at the end, rolled back on error). The jobs /
runs / job_history tables are a rebuildable cache; job_state is user-owned and this
function is structurally incapable of clearing a status, note, or date.

job_state is keyed on seen_key (role identity), so it follows a role across url
rewrites for free — no healing, no orphan parking. The only state touch here is a
deterministic display-url refresh: a state row whose seen_key has a present job adopts
that job's url; a seen_key with no present job keeps its last-known url (dormant).
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
        for r in latest_rows:
            sk = compute_seen_key(r.get("company"), r.get("title"), r.get("location"))
            url = _surrogate_url(r.get("url"), sk)
            present_urls.add(url)
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

        # 7. Refresh each state row's display url. job_state is keyed on seen_key, so a
        # url rewrite needs no healing: the state already belongs to the role. This step
        # only maintains the display url that read queries join on (jobs.url = state.url),
        # so the invariant "a present job's url belongs to at most its own seen_key's
        # state row" holds and the join can never misattribute one role's status to
        # another. updated_at is NOT bumped: a display refresh is not a user edit and
        # must not pollute the state timeline.
        #
        # 7a. A seen_key with a present job adopts that job's url (dedupe guarantees <=1;
        #     ORDER BY keeps it deterministic if that guarantee is ever violated).
        cur.execute(
            "UPDATE job_state SET url = ("
            "  SELECT j.url FROM jobs j WHERE j.seen_key = job_state.seen_key AND j.present = 1 "
            "  ORDER BY j.url LIMIT 1) "
            "WHERE EXISTS ("
            "  SELECT 1 FROM jobs j WHERE j.seen_key = job_state.seen_key AND j.present = 1)"
        )
        # 7b. A dormant seen_key (no present job) whose last-known url has since been
        #     recycled by a *different* present role detaches to NULL — otherwise the
        #     join would show this row's status on that other role's card.
        cur.execute(
            "UPDATE job_state SET url = NULL "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM jobs j WHERE j.seen_key = job_state.seen_key AND j.present = 1) "
            "AND url IN (SELECT url FROM jobs WHERE present = 1)"
        )
        healed = 0
        needs_review = 0

        # 8. Picks seeding: only for a present job whose seen_key has no state yet.
        cur.execute("SELECT seen_key FROM job_state")
        have_state = {row["seen_key"] for row in cur.fetchall()}
        for pick_file in ("picks.json", "picks_llm.json"):
            picks = _read_json(root / pick_file)
            if not isinstance(picks, list):
                continue
            for p in picks:
                if not isinstance(p, dict):
                    continue
                purl = (p.get("url") or "").strip()
                if not purl or purl not in present_urls:
                    continue
                cur.execute("SELECT seen_key FROM jobs WHERE url=?", (purl,))
                jr = cur.fetchone()
                if jr is None or jr["seen_key"] in have_state:
                    continue
                sk = jr["seen_key"]
                reason = (p.get("reason") or "").strip()
                notes = ("[pick] " + reason) if reason else "[pick]"
                seeded_at = _now()
                cur.execute(
                    "INSERT INTO job_state (seen_key, url, status, notes, starred, updated_at) "
                    "VALUES (?,?,?,?,1,?)",
                    (sk, purl, "Interested", notes, seeded_at),
                )
                # Record the seeded fields as events (new row -> old is all-NULL).
                record_field_events(
                    cur, seen_key=sk, url=purl, old={},
                    new={"status": "Interested", "notes": notes, "starred": 1},
                    source="ingest:picks", at=seeded_at,
                )
                have_state.add(sk)

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
