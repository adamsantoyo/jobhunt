"""Phase 5, W-5.4: `calibration.py` contract pins.

Written BEFORE the implementation (the 5.4 contract makes this file binding on
it). Every database is built under tmp_path via `test_source_scheduler_fakes.
make_connect` (fresh -> full canonical schema through `db.init_db`), exactly
like `test_outcome_analytics.py`. Nothing here touches webapp/app.db (repo-root
conftest.py fences JOBHUNT_DB).

The fixture helpers are deliberately the SAME shapes `test_outcome_analytics.py`
uses (`insert_posting` / `insert_version` / `insert_score` /
`insert_status_event`), because 5.4's sample counting is required to agree with
5.3's -- see `test_identity_set_matches_outcome_analytics`.
"""
import hashlib
import json
import uuid

import pytest

from backend import calibration, outcomes
from backend.calibration import calibration_report
from backend.outcome_analytics import _application_identities
from backend.tests.test_source_scheduler_fakes import make_connect

AT = "2026-08-01T12:00:00"

#: A reference moment far enough after every `_day(i)` fixture timestamp that
#: EVERY seeded application is response-mature. Model-arm tests use this so the
#: maturity window is a thing they can opt into (see the censoring tests), not
#: an invisible filter silently emptying their training set.
NOW = "2027-06-01T00:00:00"

# A band pair that splits cleanly for the empirical-cell assertions.
STRONG = "Strong match / Low competition"
WEAK = "Weak match / High competition"

#: Rows the model arm needs before it will fit at all (calibration._MIN_TRAIN
#: = 100 with a 70/30 split). Every model-arm fixture is sized off this.
BIG = 150


# --------------------------------------------------------------------------- #
# fixtures / insert helpers (mirrors test_outcome_analytics.py)
# --------------------------------------------------------------------------- #
@pytest.fixture
def conn(tmp_path):
    c = make_connect(tmp_path)()
    try:
        yield c
    finally:
        c.close()


class _FakeFamilies:
    def __init__(self, keywords):
        self.keywords = keywords


class _FakeProfile:
    def __init__(self, keywords=None, content_hash="cal-fake-profile-hash"):
        self.families = _FakeFamilies(keywords or {})
        self.content_hash = content_hash


@pytest.fixture(autouse=True)
def fake_profile(monkeypatch):
    """profile.json is gitignored and may be absent; pin a fake so role-family
    derivation is stable and never reads the developer's real profile."""
    profile = _FakeProfile()
    monkeypatch.setattr(outcomes, "_load_profile", lambda: profile)
    return profile


def insert_posting(conn, posting_id, at=AT):
    conn.execute(
        "INSERT INTO postings (posting_id, identity_status, first_seen_at, created_at) "
        "VALUES (?, 'active', ?, ?)",
        (posting_id, at, at),
    )


def insert_version(conn, posting_id, *, observed_at=AT, title="Support Engineer",
                   company="Acme Robotics", source="greenhouse", odds=WEAK, tier=1,
                   odds_score=10):
    version_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO posting_versions (posting_version_id, posting_id, version_kind, "
        "version_hash, observed_at, title, company, source, odds, tier, odds_score, "
        "payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (version_id, posting_id, "source", version_id, observed_at, title, company, source,
         odds, tier, odds_score, "{}"),
    )
    return version_id


def insert_score(conn, *, posting_version_id, posting_id, odds=WEAK, tier=1, odds_score=50,
                 created_at=AT, features_json=None,
                 profile_version_id="cal-fake-profile-hash"):
    conn.execute(
        "INSERT OR IGNORE INTO profile_versions (profile_version_id, content_hash, "
        "profile_json, created_at) VALUES (?,?,?,?)",
        (profile_version_id, profile_version_id, "{}", AT),
    )
    score_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO score_versions (score_version_id, posting_id, posting_version_id, "
        "profile_version_id, score_hash, scorer_hash, tier, odds, odds_score, created_at, "
        "superseded_at, features_json) VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?)",
        (score_id, posting_id, posting_version_id, profile_version_id, score_id, "scorer-1",
         tier, odds, odds_score, created_at,
         json.dumps(features_json) if features_json is not None else None),
    )
    return score_id


def insert_status_event(conn, seen_key, old_value, new_value, at, *, url=None, posting_id=None):
    conn.execute(
        "INSERT INTO state_events (seen_key, url, field, old_value, new_value, at, source, "
        "posting_id) VALUES (?,?, 'status', ?,?,?, 'patch', ?)",
        (seen_key, url, old_value, new_value, at, posting_id),
    )


def _day(i: int) -> str:
    """A distinct, strictly increasing full-grain timestamp per index."""
    return f"2026-{1 + (i // 28) % 12:02d}-{1 + i % 28:02d}T09:00:00"


def seed_applications(conn, n, n_responded, *, odds=WEAK, features_of=None, start=0,
                      prefix="a", with_posting=True, applied_at_of=None):
    """`n` distinct applied identities, the FIRST `n_responded` of which get an
    Applied -> Phone screen response. Each identity gets its own posting +
    version + current score (so bands and features resolve) unless
    `with_posting` is False.

    `features_of(i)` -> the features_json dict for identity `i`, or None.
    Applied timestamps strictly increase with `i` (`_day(i)`, or
    `applied_at_of(i)` when supplied -- the hook the response-maturity tests
    use to plant rows too RECENT to be believed), so the time-ordered split is
    predictable from the index.
    """
    for i in range(start, start + n):
        seen_key = f"{prefix}{i}"
        posting_id = None
        if with_posting:
            posting_id = f"p-{prefix}{i}"
            insert_posting(conn, posting_id)
            version_id = insert_version(conn, posting_id, odds=odds)
            insert_score(
                conn, posting_version_id=version_id, posting_id=posting_id, odds=odds,
                features_json=features_of(i) if features_of else None,
            )
        applied_at = applied_at_of(i) if applied_at_of else _day(i)
        insert_status_event(conn, seen_key, None, "Applied", applied_at, posting_id=posting_id)
        if i - start < n_responded:
            insert_status_event(
                conn, seen_key, "Applied", "Phone screen",
                applied_at.replace("T09:", "T18:"), posting_id=posting_id,
            )
    conn.commit()


def _walk(obj, path=""):
    """Yield (path, key, value) for every mapping entry in a nested payload."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield (f"{path}.{k}", k, v)
            yield from _walk(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            yield from _walk(v, f"{path}[{idx}]")


# --------------------------------------------------------------------------- #
# gate boundaries
# --------------------------------------------------------------------------- #
def test_empty_db_is_gated(conn):
    conn.commit()
    report = calibration_report(conn, now=AT)
    assert report["gate"] == {
        "gated": True,
        "n_applications": 0,
        "n_responses": 0,
        "thresholds": {"min_applications": 50, "min_responses": 10},
    }
    assert report["active"] == "gated"
    json.dumps(report)  # must be serializable


def test_gated_at_49_applications(conn):
    seed_applications(conn, 49, 20)
    report = calibration_report(conn, now=AT)
    assert report["gate"]["n_applications"] == 49
    assert report["gate"]["gated"] is True
    assert report["active"] == "gated"


def test_open_at_exactly_50_applications_and_10_responses(conn):
    seed_applications(conn, 50, 10)
    report = calibration_report(conn, now=AT)
    assert report["gate"]["n_applications"] == 50
    assert report["gate"]["n_responses"] == 10
    assert report["gate"]["gated"] is False
    assert report["active"] in ("empirical", "model")


def test_gated_at_9_responses(conn):
    seed_applications(conn, 60, 9)
    report = calibration_report(conn, now=AT)
    assert report["gate"]["n_applications"] == 60
    assert report["gate"]["n_responses"] == 9
    assert report["gate"]["gated"] is True


def test_open_at_10_responses(conn):
    seed_applications(conn, 60, 10)
    report = calibration_report(conn, now=AT)
    assert report["gate"]["n_responses"] == 10
    assert report["gate"]["gated"] is False


def test_thresholds_are_caller_supplied_and_echoed(conn):
    seed_applications(conn, 6, 2)
    report = calibration_report(conn, min_applications=6, min_responses=2, now=AT)
    assert report["gate"]["thresholds"] == {"min_applications": 6, "min_responses": 2}
    assert report["gate"]["gated"] is False

    stricter = calibration_report(conn, min_applications=7, min_responses=2, now=AT)
    assert stricter["gate"]["gated"] is True


# --------------------------------------------------------------------------- #
# gated payload carries NO calibrated probability
# --------------------------------------------------------------------------- #
def test_gated_payload_structurally_omits_rates(conn):
    seed_applications(conn, 49, 20)
    report = calibration_report(conn, now=AT)

    assert "empirical" not in report
    assert "model" not in report
    for path, key, _value in _walk(report):
        leaked = (
            key == "rate"
            or key.endswith("_rate")
            or "brier" in key
            or "probability" in key
            or key == "n_responded"
        )
        assert not leaked, f"gated payload leaked {path}"


def test_gated_payload_still_describes_heuristic_bands(conn):
    seed_applications(conn, 8, 4, odds=STRONG)
    report = calibration_report(conn, now=AT)
    bands = report["heuristic_bands"]
    assert [c["key"] for c in bands["by_match_band"]] == ["Strong match"]
    assert bands["by_match_band"][0]["n_applied"] == 8
    assert [c["key"] for c in bands["by_competition_band"]] == ["Low competition"]
    assert isinstance(bands["note"], str) and bands["note"]


# --------------------------------------------------------------------------- #
# empirical cell math
# --------------------------------------------------------------------------- #
def test_empirical_cell_math_on_known_fixtures(conn):
    # 8 Strong/Low applications, 6 responded; 4 Weak/High, 1 responded.
    seed_applications(conn, 8, 6, odds=STRONG, prefix="s")
    seed_applications(conn, 4, 1, odds=WEAK, prefix="w")

    report = calibration_report(conn, min_applications=12, min_responses=7, now=AT)
    assert report["gate"]["gated"] is False
    emp = report["empirical"]

    match = {c["key"]: c for c in emp["by_match_band"]}
    assert match["Strong match"] == {
        "key": "Strong match", "n": 8, "n_responded": 6, "rate": 0.75, "low_sample": False,
    }
    assert match["Weak match"] == {
        "key": "Weak match", "n": 4, "n_responded": 1, "rate": 0.25, "low_sample": True,
    }

    comp = {c["key"]: c for c in emp["by_competition_band"]}
    assert comp["Low competition"]["n"] == 8
    assert comp["High competition"]["n_responded"] == 1

    cells = {c["key"]: c for c in emp["by_cell"]}
    assert cells["Strong match / Low competition"]["rate"] == 0.75
    assert cells["Weak match / High competition"]["rate"] == 0.25
    assert report["min_sample"] == 5


def test_low_sample_flag_uses_min_sample_five(conn):
    seed_applications(conn, 4, 2, odds=STRONG, prefix="s")
    seed_applications(conn, 5, 2, odds=WEAK, prefix="w")
    report = calibration_report(conn, min_applications=9, min_responses=4, now=AT)
    match = {c["key"]: c for c in report["empirical"]["by_match_band"]}
    assert match["Strong match"]["low_sample"] is True   # n=4 < 5
    assert match["Weak match"]["low_sample"] is False    # n=5


def test_legacy_odds_without_separator_bucket_to_unknown(conn):
    # "Likely" is a legacy odds value: no " / ", so no band is derivable.
    seed_applications(conn, 6, 3, odds="Likely", prefix="l")
    # An identity with no posting at all is also unresolvable.
    seed_applications(conn, 2, 1, prefix="n", with_posting=False, start=90)

    report = calibration_report(conn, min_applications=8, min_responses=4, now=AT)
    match = {c["key"]: c for c in report["empirical"]["by_match_band"]}
    assert set(match) == {"unknown"}
    assert match["unknown"]["n"] == 8
    assert match["unknown"]["n_responded"] == 4
    cells = {c["key"]: c for c in report["empirical"]["by_cell"]}
    assert set(cells) == {"unknown / unknown"}
    assert cells["unknown / unknown"]["n"] == 8


def test_partially_unknown_cells_sort_after_fully_known_cells(conn):
    """L4: `outcome_analytics._sort_cells` pins only the literal key
    `"unknown"` last, which is right for single-band lists and wrong for cell
    keys, where HALF the key can be unknown. A half-resolved bucket must not
    outrank a fully-resolved one on count alone."""
    # The biggest bucket is half-unknown; two smaller ones are fully known.
    seed_applications(conn, 12, 4, odds="Strong match / ", prefix="h")
    seed_applications(conn, 5, 2, odds=STRONG, prefix="s")
    seed_applications(conn, 3, 1, odds=WEAK, prefix="w", start=40)
    seed_applications(conn, 4, 1, odds="Likely", prefix="l", start=60)

    report = calibration_report(conn, min_applications=24, min_responses=5, now=AT)
    keys = [c["key"] for c in report["empirical"]["by_cell"]]
    assert keys == [
        "Strong match / Low competition",   # n=5, fully known
        "Weak match / High competition",    # n=3, fully known
        "Strong match / unknown",           # n=12, but half unknown
        "unknown / unknown",                # n=4
    ]


def test_sort_cells_partial_unknown_last_is_count_ordered_within_each_group():
    cells = [
        {"key": "a / unknown", "n": 1},
        {"key": "b / y", "n": 2},
        {"key": "unknown / z", "n": 9},
        {"key": "a / x", "n": 2},
        {"key": "c / w", "n": 5},
    ]
    ordered = calibration._sort_cells_partial_unknown_last(cells, "n")
    assert [c["key"] for c in ordered] == [
        "c / w",          # known, n=5
        "a / x",          # known, n=2, key ASC breaks the tie
        "b / y",          # known, n=2
        "unknown / z",    # unknown-bearing, n=9
        "a / unknown",    # unknown-bearing, n=1
    ]


def test_unknown_band_sorts_last(conn):
    seed_applications(conn, 3, 1, odds="Likely", prefix="l")
    seed_applications(conn, 6, 2, odds=STRONG, prefix="s")
    report = calibration_report(conn, min_applications=9, min_responses=3, now=AT)
    keys = [c["key"] for c in report["empirical"]["by_match_band"]]
    assert keys[-1] == "unknown"


# --------------------------------------------------------------------------- #
# sample counting reuses 5.3's definitions
# --------------------------------------------------------------------------- #
def test_response_definition_matches_funnel():
    from backend.routers import funnel

    assert calibration._RESPONSE_STAGES == funnel._RESPONSE_STAGES
    assert calibration._RESPONSE_STAGES == ("Phone screen", "Interview", "Offer", "Rejected")


def test_passed_is_not_a_response(conn):
    insert_status_event(conn, "sk1", None, "Applied", "2026-08-01T09:00:00")
    insert_status_event(conn, "sk1", "Applied", "Passed", "2026-08-05T09:00:00")
    conn.commit()
    report = calibration_report(conn, now=AT)
    assert report["gate"] == {
        "gated": True, "n_applications": 1, "n_responses": 0,
        "thresholds": {"min_applications": 50, "min_responses": 10},
    }


def test_identity_set_matches_outcome_analytics(conn, fake_profile):
    """The gate's denominator and the model arm's row set must be the SAME
    applied-identity set 5.3 counts -- not a third definition."""
    seed_applications(conn, 12, 5, odds=STRONG, prefix="s")
    seed_applications(conn, 3, 1, prefix="n", with_posting=False, start=90)
    # Two seen_keys pointing at one posting: 5.3 merges them into one identity.
    insert_posting(conn, "p-dupe")
    conn.commit()
    insert_status_event(conn, "d1", None, "Applied", "2026-09-01T09:00:00", posting_id="p-dupe")
    insert_status_event(conn, "d2", None, "Applied", "2026-09-02T09:00:00", posting_id="p-dupe")
    conn.commit()

    identities = _application_identities(conn, fake_profile)
    rows = calibration._applied_rows(conn, fake_profile)
    report = calibration_report(conn, now=AT)

    # Equal LENGTH is not equal SET: two definitions can disagree row-for-row
    # and still count the same total. Compare the multiset of the facts both
    # sides derive -- the response label and both bands -- so a row swapped for
    # a different row, or a response attributed to the wrong identity, fails.
    def facts(items):
        return sorted(
            (bool(i["responded"]), i["match_band"] or "", i["competition_band"] or "")
            for i in items
        )

    assert facts(rows) == facts(identities)
    assert len(rows) == len(identities)
    # ... and the identity keys themselves are distinct (no silent duplicate
    # standing in for a missing row).
    assert len({r["identity_key"] for r in rows}) == len(rows)
    assert report["gate"]["n_applications"] == len(identities)
    assert report["gate"]["n_responses"] == sum(1 for i in identities if i["responded"])


def test_applied_rows_response_requires_old_value_applied(conn, fake_profile):
    """M3: the response rule is a TRANSITION out of Applied, not the mere
    appearance of a response stage anywhere in the stream. Here the applicant
    passed first and the company rejected afterwards -- `Passed -> Rejected`.
    Ignoring `old_value` would score that as a response."""
    insert_status_event(conn, "m3", None, "Applied", "2026-03-01T09:00:00")
    insert_status_event(conn, "m3", "Applied", "Passed", "2026-03-05T09:00:00")
    insert_status_event(conn, "m3", "Passed", "Rejected", "2026-03-09T09:00:00")
    conn.commit()

    rows = calibration._applied_rows(conn, fake_profile)
    assert len(rows) == 1
    assert rows[0]["responded"] is False

    # ... and the genuine transition IS a response, so the pin is not just
    # "always False".
    insert_status_event(conn, "m3b", None, "Applied", "2026-03-01T09:00:00")
    insert_status_event(conn, "m3b", "Applied", "Rejected", "2026-03-09T09:00:00")
    conn.commit()
    rows = {r["identity_key"]: r for r in calibration._applied_rows(conn, fake_profile)}
    assert rows["sk:m3b"]["responded"] is True


# --------------------------------------------------------------------------- #
# time-ordered split
# --------------------------------------------------------------------------- #
def test_time_ordered_split_is_oldest_train_newest_heldout():
    rows = [{"applied_at": _day(i), "identity_key": f"k{i:02d}"} for i in range(20)]
    train, heldout = calibration._time_ordered_split(rows)
    assert len(train) == 14 and len(heldout) == 6  # int(20 * 0.7)
    assert [r["identity_key"] for r in train] == [f"k{i:02d}" for i in range(14)]
    assert [r["identity_key"] for r in heldout] == [f"k{i:02d}" for i in range(14, 20)]


def test_time_ordered_split_is_permutation_invariant():
    rows = [{"applied_at": _day(i), "identity_key": f"k{i:02d}"} for i in range(20)]
    shuffled = rows[7:] + rows[:7]
    train_a, held_a = calibration._time_ordered_split(rows)
    train_b, held_b = calibration._time_ordered_split(shuffled)
    assert [r["identity_key"] for r in train_a] == [r["identity_key"] for r in train_b]
    assert [r["identity_key"] for r in held_a] == [r["identity_key"] for r in held_b]


def test_time_ordered_split_breaks_timestamp_ties_deterministically():
    rows = [{"applied_at": AT, "identity_key": k} for k in ("c", "a", "b")]
    train, heldout = calibration._time_ordered_split(rows)
    assert [r["identity_key"] for r in train + heldout] == ["a", "b", "c"]


def test_model_split_puts_newest_applications_in_heldout(conn):
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["n_train"] == 105          # int(150 * 0.7)
    assert model["n_heldout"] == 45
    assert model["n_train"] + model["n_heldout"] == BIG


# --------------------------------------------------------------------------- #
# model arm
# --------------------------------------------------------------------------- #
def _separable_features(i, n_responded):
    """A perfectly informative feature: `skills_strong` is 1.0 exactly for the
    identities `seed_applications` gives a response to."""
    responded = i < n_responded
    return {
        "score_row": {"raw_score": 60.0 if responded else 20.0},
        "hireability": {"skills_strong": 1.0 if responded else 0.0},
    }


def _noise_features(i):
    """Deterministic noise: a hash of the index, uncorrelated with the label by
    construction. Never `random` -- a flaky calibration test would be worse
    than none."""
    digest = hashlib.sha256(f"calibration-noise-{i}".encode()).digest()
    return {
        "score_row": {"raw_score": float(digest[0])},
        "hireability": {"skills_strong": float(digest[1] % 2)},
    }


def test_model_admitted_on_separable_dataset(conn):
    # Bands are uninformative (one band for everyone) and the feature is
    # perfect -> the model must beat BOTH floors.
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)

    model = report["model"]
    assert model["attempted"] is True
    assert model["admitted"] is True
    assert report["active"] == "model"
    assert model["brier_model"] < model["brier_empirical"]
    assert model["brier_model"] < model["brier_intercept"]
    # The comparison is always evidence, so every number is reported.
    assert {"brier_model", "brier_empirical", "brier_intercept",
            "n_train", "n_heldout"} <= set(model)
    # ... and the empirical section stays present even when the model wins.
    assert "empirical" in report


def test_model_rejected_on_noise_dataset(conn):
    # Bands perfectly predict the label; the features are hash noise. The model
    # cannot beat the baseline, and the losing comparison is still reported.
    seed_applications(conn, 75, 75, odds=STRONG, prefix="r", features_of=_noise_features)
    seed_applications(conn, 75, 0, odds=WEAK, prefix="x", features_of=_noise_features, start=150)

    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["attempted"] is True
    assert model["admitted"] is False
    assert report["active"] == "empirical"
    assert model["n_heldout"] >= 30


def test_baseline_band_rates_are_fitted_on_train_rows_only(conn):
    """The baseline must not peek at the held-out rows it is scored against.
    Fixture: one band for everyone, 60/105 responded in train, 45/45 in
    held-out. A train-only fit predicts 60/105 for every held-out row (all of
    which responded), so brier_empirical == (1 - 60/105)^2. A baseline that
    pooled all 150 rows would predict 105/150 and score a smaller, dishonest
    number."""
    seed_applications(conn, 105, 60, odds=STRONG, prefix="a", features_of=_noise_features)
    seed_applications(conn, 45, 45, odds=STRONG, prefix="b", start=105,
                      features_of=_noise_features)

    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["n_train"] == 105 and model["n_heldout"] == 45
    assert model["brier_empirical"] == pytest.approx((1.0 - 60.0 / 105.0) ** 2)


def test_admission_requires_strictly_beating_both_floors():
    # (model, empirical, intercept)
    assert calibration._admits(0.10, 0.20, 0.25) is True
    assert calibration._admits(0.20, 0.20, 0.25) is False   # tie on the bands
    assert calibration._admits(0.30, 0.20, 0.25) is False   # loses to the bands
    assert calibration._admits(0.22, 0.30, 0.22) is False   # tie on the intercept
    assert calibration._admits(0.24, 0.30, 0.22) is False   # loses to the intercept


def test_model_losing_to_intercept_is_rejected_even_when_it_beats_the_bands(conn):
    """C1, the reviewer's demonstration, as a fixture.

    Train (oldest 105): 85 Weak/High rows that NEVER responded, 20 Strong/Low
    rows that always did. Held out (newest 45): Weak/High rows that ALL
    responded -- the band's train rate is exactly 0.0, so the cell baseline
    predicts 0.0 for every one of them and scores a Brier of 1.0, the worst
    score arithmetically available.

    The model is handed the feature that separates TRAIN (it fires only for
    the Strong/Low responders), so it predicts near-zero for the held-out rows
    too: catastrophic, but strictly less catastrophic than 1.0. Under a
    single-floor rule that is an admission. The intercept -- the flat train
    base rate 20/105 -- beats both, and it knows nothing at all. So: rejected.
    """
    seed_applications(conn, 85, 0, odds=WEAK, prefix="t0",
                      features_of=lambda i: _separable_features(i, 0))
    seed_applications(conn, 20, 20, odds=STRONG, prefix="t1", start=85,
                      features_of=lambda i: _separable_features(i, 1000))
    seed_applications(conn, 45, 45, odds=WEAK, prefix="h", start=105,
                      features_of=lambda i: _separable_features(i, 0))

    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["n_train"] == 105 and model["n_heldout"] == 45
    # The fixture really is the pathological one: the model beats the bands...
    assert model["brier_empirical"] == pytest.approx(1.0)
    assert model["brier_model"] < model["brier_empirical"]
    # ... and still loses to one number that knows nothing.
    assert model["brier_intercept"] == pytest.approx((1.0 - 20.0 / 105.0) ** 2)
    assert model["brier_model"] > model["brier_intercept"]
    assert model["admitted"] is False
    assert report["active"] == "empirical"


def test_model_not_attempted_when_heldout_below_minimum(conn):
    # 90 applications -> n_heldout = 28 < 30: the comparison would be noise.
    seed_applications(conn, 90, 45, odds=STRONG, features_of=lambda i: _separable_features(i, 45))
    report = calibration_report(conn, min_applications=90, min_responses=10, now=NOW)
    model = report["model"]
    assert model["attempted"] is False
    assert model["reason"] == "insufficient-holdout"
    assert model["n_train"] == 62 and model["n_heldout"] == 28
    assert model["min_heldout"] == 30
    assert "brier_model" not in model
    assert "brier_empirical" not in model
    assert "brier_intercept" not in model
    assert report["active"] == "empirical"


def test_model_not_attempted_when_train_below_minimum(conn):
    """37 unregularized features fitted on a few dozen rows is a coin flip, and
    a coin flip that lands heads would be ADMITTED. 120 applications clear the
    held-out floor (36 >= 30) and still fail the train floor (84 < 100)."""
    seed_applications(conn, 120, 60, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 60))
    report = calibration_report(conn, min_applications=120, min_responses=10, now=NOW)
    model = report["model"]
    assert model["attempted"] is False
    assert model["reason"] == "insufficient-train"
    assert model["n_train"] == 84 and model["n_heldout"] == 36
    assert model["min_train"] == 100
    assert "brier_model" not in model
    assert report["active"] == "empirical"


def test_model_not_attempted_without_features(conn):
    seed_applications(conn, BIG, 12, odds=STRONG)  # features_json NULL everywhere
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["attempted"] is False
    assert model["reason"] == "no-featured-rows"
    assert report["active"] == "empirical"


def test_model_arm_absent_while_gated(conn):
    seed_applications(conn, 40, 20, odds=STRONG, features_of=lambda i: _separable_features(i, 20))
    report = calibration_report(conn, now=NOW)  # default 50/10 -> gated
    assert report["gate"]["gated"] is True
    assert "model" not in report


# --------------------------------------------------------------------------- #
# response maturity (right-censoring)
# --------------------------------------------------------------------------- #
def _recent(i: int) -> str:
    """An applied-at only a few days before `NOW`: the application is real, but
    its "no response" label is not yet believable."""
    # 2027-05-12 .. 2027-05-26, i.e. 6 to 20 days before NOW -- every one of
    # them inside the 21-day window, none of them on the boundary.
    return f"2027-05-{12 + (i % 15):02d}T09:00:00"


def test_maturity_window_excludes_recent_rows_from_the_model_arm(conn):
    """Counts, not just the verdict: mature rows train and are held out,
    immature rows are excluded from BOTH and reported as `n_immature`."""
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    seed_applications(conn, 40, 0, odds=STRONG, prefix="new", start=300,
                      features_of=_noise_features, applied_at_of=_recent)

    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["n_applied_considered"] == BIG + 40
    assert model["n_immature"] == 40
    assert model["n_featured"] == BIG
    assert model["n_train"] + model["n_heldout"] == BIG
    assert model["maturity_days"] == 21


def test_maturity_boundary_is_exactly_twenty_one_days(conn):
    # 21 days before NOW is mature (inclusive); 20 days is not.
    assert calibration._is_mature("2027-05-11T00:00:00", NOW) is True
    assert calibration._is_mature("2027-05-12T00:00:00", NOW) is False
    assert calibration.RESPONSE_MATURITY_DAYS == 21


def test_immature_rows_cannot_flip_the_admission_decision(conn):
    """The censoring failure mode, end to end. The mature history is separable
    and would be admitted. The recent rows carry the responder feature but are
    labelled negative -- not because the feature is wrong, but because nobody
    has replied YET. Being newest, a time-ordered split would dump them into
    held-out, where they would look like the model's own errors.

    Same dataset, with and without them, must reach the same verdict."""
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    baseline = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    assert baseline["model"]["admitted"] is True

    seed_applications(conn, 60, 0, odds=STRONG, prefix="new", start=300,
                      features_of=lambda i: _separable_features(i, 1000),
                      applied_at_of=_recent)
    after = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)

    assert after["model"]["n_immature"] == 60
    assert after["model"]["admitted"] is True
    assert after["model"]["brier_model"] == pytest.approx(baseline["model"]["brier_model"])
    assert after["model"]["n_train"] == baseline["model"]["n_train"]


def test_model_arm_not_attempted_without_a_reference_moment(conn):
    """Maturity cannot be judged with no `now`, and the module will not read a
    clock to invent one -- so the arm refuses rather than attempting on an
    unjudgeable window."""
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=None)
    model = report["model"]
    assert model["attempted"] is False
    assert model["reason"] == "no-now"
    assert "brier_model" not in model
    assert report["active"] == "empirical"
    # ... and the empirical section is unaffected: it describes ALL the data.
    assert report["empirical"]["n_applications"] == BIG


def test_empirical_cells_still_count_immature_applications(conn):
    """The maturity window is a MODEL-arm rule. The empirical section is
    descriptive of everything recorded, and hiding recent applications from it
    would understate the denominator the reader is looking at."""
    seed_applications(conn, 10, 5, odds=STRONG, prefix="old")
    seed_applications(conn, 6, 0, odds=STRONG, prefix="new", start=300,
                      applied_at_of=_recent)
    report = calibration_report(conn, min_applications=16, min_responses=5, now=NOW)
    cells = {c["key"]: c for c in report["empirical"]["by_match_band"]}
    assert cells["Strong match"]["n"] == 16
    assert cells["Strong match"]["n_responded"] == 5


# --------------------------------------------------------------------------- #
# row accounting
# --------------------------------------------------------------------------- #
def test_row_accounting_is_reported_in_every_model_state(conn):
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    keys = {"n_applied_considered", "n_immature", "n_featured",
            "n_dropped_no_features", "n_features", "maturity_days"}

    attempted = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    assert keys <= set(attempted["model"])
    assert attempted["model"]["attempted"] is True

    no_now = calibration_report(conn, min_applications=BIG, min_responses=10, now=None)
    assert keys <= set(no_now["model"])
    assert no_now["model"]["n_applied_considered"] == BIG


def test_row_accounting_counts_rows_dropped_for_missing_features(conn):
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _separable_features(i, 75))
    seed_applications(conn, 25, 0, odds=STRONG, prefix="nf", start=200)  # no features_json
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["n_applied_considered"] == BIG + 25
    assert model["n_immature"] == 0
    assert model["n_dropped_no_features"] == 25
    assert model["n_featured"] == BIG
    # The accounting must close, with no unexplained residue.
    assert (
        model["n_applied_considered"]
        - model["n_immature"]
        - model["n_dropped_no_features"]
        == model["n_featured"]
    )
    assert model["n_train"] + model["n_heldout"] == model["n_featured"]


# --------------------------------------------------------------------------- #
# leakage pins (H1): the held-out rows must not reach the fit, in EITHER of the
# two ways they can -- through the labels, or through the standardizer.
# --------------------------------------------------------------------------- #
def _one_signal_features(i, n_responded, *, outlier_at=None):
    """`raw_score` is the ONLY informative column (`skills_strong` is pinned
    flat, so no second feature can rescue the fit), plus optionally ONE row
    whose raw_score is five orders of magnitude larger than anything else.

    A single feature is the point: if the standardizer is fitted on
    train+heldout, that outlier sets the column scale, every train value
    collapses to within ~1e-4 of the same standardized number, and the model
    has nothing left to learn from. With a second clean separable column the
    fit would survive the sabotage and the leak would go unnoticed."""
    responded = i < n_responded
    raw = 60.0 if responded else 20.0
    if outlier_at is not None and i == outlier_at:
        raw = 1_000_000.0
    return {"score_row": {"raw_score": raw}, "hireability": {"skills_strong": 0.0}}


def test_standardizer_is_fitted_on_train_rows_only(conn):
    """MUTATION PIN (H1a): `_standardizer(train_x_raw + heldout_x_raw)`.

    Fitted on train only, the column scale is the train spread, the single
    feature separates, and the model wins outright. Fitted on train+heldout,
    the last held-out row's 1e6 sets the scale, the standardized train values
    collapse into each other, and the fit degenerates toward the intercept --
    which the intercept floor then refuses to admit."""
    seed_applications(conn, BIG, 75, odds=STRONG,
                      features_of=lambda i: _one_signal_features(i, 75, outlier_at=BIG - 1))
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["attempted"] is True
    assert model["admitted"] is True
    assert model["brier_model"] < 0.5 * model["brier_intercept"]


def _inverted_features(i, *, heldout_from, heldout_responders_until):
    """Train: the feature fires for RESPONDERS. Held out: the same feature
    fires for NON-responders. The two halves state opposite rules about the
    same column."""
    if i < heldout_from:
        fires = i < 52                       # train responders
    else:
        fires = i >= heldout_responders_until  # held-out NON-responders
    return {
        "score_row": {"raw_score": 60.0 if fires else 20.0},
        "hireability": {"skills_strong": 1.0 if fires else 0.0},
    }


def test_heldout_labels_never_reach_training(conn):
    """MUTATION PIN (H1b): `_fit_logistic(train_x + heldout_x, train_y +
    heldout_y)`.

    An honest model carries the TRAIN rule into the held-out set, where the
    rule is inverted, and is therefore confidently wrong on nearly every row --
    a Brier near 1. A model that trained on the held-out labels has seen the
    contradiction, hedges toward the middle, and lands near 0.25. The gap is
    not subtle, and neither is the verdict: honest, this model is rejected."""
    # Train = oldest 105 (responders are the first 52), held out = newest 45
    # (responders are the first 22 of those, i.e. i in [105, 127)).
    seed_applications(conn, 105, 52, odds=STRONG, prefix="tr",
                      features_of=lambda i: _inverted_features(
                          i, heldout_from=105, heldout_responders_until=127))
    seed_applications(conn, 45, 22, odds=STRONG, prefix="ho", start=105,
                      features_of=lambda i: _inverted_features(
                          i, heldout_from=105, heldout_responders_until=127))

    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["n_train"] == 105 and model["n_heldout"] == 45
    assert model["brier_model"] > 0.6, "held-out labels appear to have reached the fit"
    assert model["admitted"] is False


# --------------------------------------------------------------------------- #
# numerical guards
# --------------------------------------------------------------------------- #
def test_sigmoid_clamps_extreme_margins_without_overflowing():
    for z in (1e9, -1e9, 1e300, -1e300, 60.0, -60.0, 0.0):
        p = calibration._sigmoid(z)
        assert 0.0 <= p <= 1.0
    assert calibration._sigmoid(1e9) == pytest.approx(1.0, abs=1e-9)
    assert calibration._sigmoid(-1e9) == pytest.approx(0.0, abs=1e-9)
    assert calibration._sigmoid(0.0) == 0.5


def test_predict_survives_extreme_feature_values():
    n_cols = len(calibration._FEATURE_NAMES)
    for value in (1e12, -1e12):
        p = calibration._predict([value] * n_cols, [1.0] * n_cols, 0.0)
        assert 0.0 <= p <= 1.0


def test_extreme_feature_values_do_not_break_the_report(conn):
    seed_applications(
        conn, BIG, 75, odds=STRONG,
        features_of=lambda i: {
            "score_row": {"raw_score": 1e12 if i < 75 else -1e12},
            "hireability": {"skills_strong": 1e12 if i < 75 else 0.0},
        },
    )
    report = calibration_report(conn, min_applications=BIG, min_responses=10, now=NOW)
    model = report["model"]
    assert model["attempted"] is True
    assert 0.0 <= model["brier_model"] <= 1.0


# --------------------------------------------------------------------------- #
# determinism
# --------------------------------------------------------------------------- #
def test_two_runs_are_byte_identical(conn):
    seed_applications(conn, 40, 20, odds=STRONG, features_of=lambda i: _separable_features(i, 20))
    seed_applications(conn, 10, 3, odds="Likely", prefix="l", start=60)
    kwargs = dict(min_applications=40, min_responses=10, now=AT)
    first = json.dumps(calibration_report(conn, **kwargs), sort_keys=False)
    second = json.dumps(calibration_report(conn, **kwargs), sort_keys=False)
    assert first == second


def test_module_reads_no_clock_and_no_randomness():
    import inspect

    source = inspect.getsource(calibration)
    body = "\n".join(
        line for line in source.splitlines()
        if not line.lstrip().startswith("#")
    )
    for forbidden in ("datetime.now", "utcnow", "time.time", "random.", "import random"):
        assert forbidden not in body, f"calibration.py must not use {forbidden}"


def test_now_is_injected_and_echoed(conn):
    conn.commit()
    assert calibration_report(conn, now="2026-08-05T00:00:00")["generated_at"] == \
        "2026-08-05T00:00:00"
    assert calibration_report(conn)["generated_at"] is None
