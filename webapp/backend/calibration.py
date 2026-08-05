"""Phase 5, W-5.4: calibration gating.

The odds bands this app shows (`"<match> / <competition>"`) are HEURISTIC
claims produced by `rubric.py`'s weights, not measured probabilities. This
module answers one question honestly: does the recorded outcome history
justify turning those labels into numbers yet, and if so, what do the numbers
actually say?

Three states, in increasing order of what the payload is allowed to assert:

  gated      Not enough applications, or not enough responses, to say anything
             about a rate without lying. The payload contains the GATE RECORD
             and a description of the heuristic bands -- and NO BAND-LEVEL
             rate of any kind. Absence is structural: the `empirical` and
             `model` keys are not present at all, rather than present and
             null. A null rate still invites a consumer to render "--%"; a
             missing key cannot.

             The gate record itself does publish `n_applications` and
             `n_responses` -- deliberately, and it is not a leak. Reporting
             PROGRESS TOWARD the thresholds is the gate's entire purpose; a
             consumer that cannot see "42 of 50, 7 of 10" cannot tell a gated
             app from a broken one. What the gated payload withholds is the
             per-band and per-cell breakdown -- the thing that would let a
             reader attach a number to a specific job card.
  empirical  Open. Response rates measured per match band, per competition
             band, and per match x competition cell, each carrying its own
             denominator and a `low_sample` flag.
  model      Open, AND a logistic regression over the scorer's own feature
             vectors beat BOTH the empirical band rates and an intercept-only
             constant on held-out, response-MATURE applications.

Determinism is a hard requirement (`test_two_runs_are_byte_identical`, and
`test_module_reads_no_clock_and_no_randomness` pins the mechanism, not just
the outcome): no clock is ever read here (`now` is injected by the caller, and
the maturity window below is measured against THAT, not against a clock read),
and nothing is random -- the model's weights start at zero, it
runs a FIXED iteration count, and the train/held-out split is by TIME, never
by sampling.

Read-only: every statement below is a SELECT. Like `funnel.py` and
`outcome_analytics.py`, this module never writes and never commits.

ONE definition of "application" and "response"
----------------------------------------------
The gate's denominator and numerator come from
`outcome_analytics._application_identities` -- W-5.3's already-tested applied
identity resolution (posting_id-first, seen_keys resolving to one posting
merged into one identity) -- and its response rule is `funnel.py`'s
`_RESPONSE_STAGES` (Applied -> Phone screen / Interview / Offer / Rejected;
Passed is the applicant giving up, not the company responding). This module
mints NEITHER definition; `test_identity_set_matches_outcome_analytics` and
`test_response_definition_matches_funnel` pin that.

`_applied_rows` below looks like a second identity pass, and is not a second
DEFINITION: it applies `outcome_analytics`'s own identity helper
(`_seen_key_posting_ids`) and its own response constant to produce the two
facts `_application_identities` does not return but the model arm needs --
`applied_at` (the time-ordered split key) and `posting_id` (the bridge to
`score_versions.features_json`). The row SET is pinned equal to
`_application_identities`'s by test. It runs only when the gate is OPEN, so
the gated path stays single-pass.

What the model arm has to beat, and what it is allowed to look at
-----------------------------------------------------------------
TWO floors, both strict, both on the SAME held-out rows: the empirical
match x competition cell rates (fitted on train only), and an INTERCEPT-ONLY
constant equal to the train-set overall response rate. The second floor is not
redundant. A cell baseline can be arbitrarily bad -- a held-out row whose cell
responded 0/85 in train is predicted 0.0, so a held-out responder scores a
Brier of 1.0 against it -- and a model that is merely "less catastrophic than
that" (0.99) would clear a single-floor rule while being worse than the
one-number answer "everyone responds at the base rate". Beating a bad baseline
is not evidence; beating the base rate is the minimum bar for claiming the
features carry signal. `brier_intercept` is reported next to `brier_model` and
`brier_empirical` whenever the comparison is computed.

RIGHT-CENSORING. A response arrives days or weeks after the apply, so a recent
application labelled "no response" may only be labelled "no response YET".
Because the split is by TIME, those unripe rows land disproportionately in the
held-out set, where a systematic excess of false negatives makes both the model
and the baselines look wrong in ways that say nothing about either. The model
arm therefore only considers applications applied at least
`RESPONSE_MATURITY_DAYS` before `now` -- train rows, held-out rows, and both
baselines' evaluation rows alike. Excluded rows are COUNTED (`n_immature`), not
hidden. With no `now` the maturity of a row cannot be judged at all, so the
model arm is not attempted (`reason: "no-now"`) rather than attempted on an
unjudgeable window. The empirical band cells are unaffected: they are
descriptive of ALL recorded data, and carry their own `n` for the reader.

KNOWN BIAS in the feature vectors (accepted, documented, not yet fixed). A row's
features come from the posting's CURRENT score -- `outcomes._current_score` --
not from the score that was live at the moment of applying. Two consequences:
a posting re-scored AFTER its outcome contributes post-outcome features, and a
posting with no current score at all drops out of the model arm entirely
(`n_dropped_no_features`). The second is the sharper one, because "no current
score" correlates with the posting having been closed or delisted, which
correlates with outcome. Point-in-time features (resolving the score version
live at `applied_at`) are deferred until the evidence gate first opens on real
data -- there is no point paying for point-in-time reconstruction while the arm
is still reporting `insufficient-train`. Every row-accounting number the arm
would need to audit that bias (`n_applied_considered`, `n_immature`,
`n_featured`, `n_dropped_no_features`) is in the payload today.
"""
from __future__ import annotations

import json
import math
import sqlite3

from . import outcome_analytics, outcomes
from .outcome_analytics import (
    _RESPONSE_STAGES,
    _application_identities,
    _chunks,
    _group,
    _in_clause,
    _parse,
    _seen_key_posting_ids,
    _sort_cells,
)

# `candidate_profile` lives at the repo root; importing `outcomes` (above)
# performs the sys.path insert. Imported here for the CLOSED feature
# vocabularies only -- the model must have a fixed, ordered feature list, or
# two runs over the same data could disagree on what column 7 means.
import candidate_profile  # noqa: E402

#: `low_sample` threshold on every empirical cell, identical to
#: `outcome_analytics.outcome_analytics`'s default. A flagged cell is still
#: real data; the flag says "not trustworthy alone", not "hidden".
_MIN_SAMPLE = 5

#: Fraction of applications (oldest-first) used to TRAIN. The remainder is
#: held out. Time-ordered, never sampled: a calibration model is only useful
#: if it predicts applications it has not seen, and the applications it has
#: not seen are the FUTURE ones.
_TRAIN_FRACTION = 0.7

#: Below this many held-out rows the Brier comparison is noise, so the model
#: arm is not attempted at all rather than admitted or rejected on a coin flip.
_MIN_HELDOUT = 30

#: ... and below this many TRAIN rows the fit itself is noise. There are 37
#: unregularized features; fitting them on a few dozen rows is a coin flip
#: dressed as a measurement, and a coin flip that lands heads gets ADMITTED.
#: The floor is on evidence, not on convergence -- the optimizer will happily
#: "converge" on 35 rows.
_MIN_TRAIN = 100

#: How long an application must have been outstanding before its "no response"
#: label is believable. Responses arrive on a lag; a row applied yesterday and
#: labelled negative is mostly telling us the calendar. Rows younger than this
#: are excluded from the MODEL arm (and counted as `n_immature`); the empirical
#: cells still describe all data.
RESPONSE_MATURITY_DAYS = 21

#: Fixed optimizer schedule -- no early stopping, no tolerance, no adaptive
#: anything. Same data in, same weights out, every time.
_ITERATIONS = 400
_LEARNING_RATE = 0.5

#: Ordered feature vocabulary: the scorer's own closed sets. `sorted()` here is
#: hygiene rather than a correctness requirement -- the fit is permutation-
#: EQUIVARIANT (zero-init weights, batch gradient descent), so permuting the
#: columns permutes the weights and leaves every reported number the same to
#: within floating-point accumulation order. What sorting buys is a column list
#: that is legible and fixed: the sources are `frozenset`s, whose iteration
#: order is not stable across processes, and pinning it also pins that last
#: hair of summation order.
_FEATURE_NAMES = tuple(
    [("score_row", name) for name in sorted(candidate_profile.REQUIRED_SCORE_ROW_FEATURES)]
    + [("hireability", name) for name in sorted(candidate_profile.REQUIRED_HIREABILITY_FEATURES)]
)

_UNKNOWN = "unknown"

_BAND_NOTE = (
    "Match and competition labels are heuristic rubric outputs, not measured "
    "probabilities. Counts below describe how many applications carried each "
    "label; no response rate is reported until the calibration gate opens."
)


# --------------------------------------------------------------------------- #
# gate + empirical section
# --------------------------------------------------------------------------- #
def _sort_cells_partial_unknown_last(cells: list, count_key: str) -> list:
    """`_sort_cells` for CELL keys, which are composite (`"<match> / <comp>"`)
    and therefore have a state W-5.3's helper never sees: PARTIALLY unknown.

    `outcome_analytics._sort_cells` pins only the literal key `"unknown"` last,
    so `"Strong match / unknown"` competes on count with fully-known cells and
    can outrank them -- a half-resolved bucket presented as the headline
    evidence. Sorted here instead: any key mentioning `unknown` sorts after
    every fully-known key, and within each group the W-5.3 rule (count DESC,
    key ASC) applies unchanged. A local wrapper, deliberately: 5.3's helper is
    shared by shipped endpoints and is not 5.4's to change."""
    def ordered(subset):
        return sorted(subset, key=lambda c: (-c[count_key], c["key"]))

    known = ordered([c for c in cells if _UNKNOWN not in c["key"]])
    partial = ordered([c for c in cells if _UNKNOWN in c["key"]])
    return known + partial


def _band_key(row, field: str) -> str:
    return row[field] or _UNKNOWN


def _cell_key(row) -> str:
    """The match x competition cell key, written in the same
    `"<match> / <competition>"` shape the odds string itself uses so a reader
    can match a cell against a job card without a translation step. An
    unresolvable side reads `unknown` on that side only -- an application whose
    match band is known but whose competition band is not is not the same
    evidence as one where neither resolved."""
    return f"{_band_key(row, 'match_band')} / {_band_key(row, 'competition_band')}"


def _rate_cell(key: str, rows: list) -> dict:
    """One empirical cell. `n` is applications, `n_responded` is applications
    that got a response, and `rate` is always `n_responded / n` -- one
    denominator, stated on every cell, so cells are comparable with each
    other."""
    n = len(rows)
    n_responded = sum(1 for r in rows if r["responded"])
    return {
        "key": key,
        "n": n,
        "n_responded": n_responded,
        "rate": (n_responded / n) if n else None,
        "low_sample": n < _MIN_SAMPLE,
    }


def _empirical(identities: list) -> dict:
    """Response rates per match band, per competition band, and per cell.
    `_sort_cells` (W-5.3's) orders the single-band lists by count DESC then key
    ASC with the literal `"unknown"` bucket pinned last; `by_cell`, whose keys
    are composite and can be HALF unknown, uses the local wrapper above."""
    return {
        "min_sample": _MIN_SAMPLE,
        "n_applications": len(identities),
        "by_match_band": _sort_cells(
            [
                _rate_cell(k, rows)
                for k, rows in _group(identities, lambda r: _band_key(r, "match_band")).items()
            ],
            "n",
        ),
        "by_competition_band": _sort_cells(
            [
                _rate_cell(k, rows)
                for k, rows in _group(
                    identities, lambda r: _band_key(r, "competition_band")
                ).items()
            ],
            "n",
        ),
        "by_cell": _sort_cells_partial_unknown_last(
            [_rate_cell(k, rows) for k, rows in _group(identities, _cell_key).items()], "n"
        ),
    }


def _heuristic_bands(identities: list) -> dict:
    """What the heuristic labels claimed, with counts only -- NO rate, and no
    numerator. This section is present in every state including `gated`, so it
    must not smuggle a response rate past the gate: `n_applied` alone cannot be
    turned into one."""
    return {
        "note": _BAND_NOTE,
        "by_match_band": _sort_cells(
            [
                {"key": k, "n_applied": len(rows)}
                for k, rows in _group(identities, lambda r: _band_key(r, "match_band")).items()
            ],
            "n_applied",
        ),
        "by_competition_band": _sort_cells(
            [
                {"key": k, "n_applied": len(rows)}
                for k, rows in _group(
                    identities, lambda r: _band_key(r, "competition_band")
                ).items()
            ],
            "n_applied",
        ),
    }


# --------------------------------------------------------------------------- #
# model arm: rows, features, split
# --------------------------------------------------------------------------- #
def _applied_rows(conn: sqlite3.Connection, profile) -> list:
    """One row per applied identity, carrying the two facts the model arm needs
    that `_application_identities` does not return: `applied_at` (first Applied
    event, the split key) and `posting_id` (the features bridge).

    Identity resolution is `outcome_analytics._seen_key_posting_ids` -- the
    SAME helper `_application_identities` uses, so the two agree row-for-row by
    construction, and `_RESPONSE_STAGES` is the same constant. `identity_key`
    is a stable string used only to break exact-timestamp ties in the split.
    """
    event_rows = conn.execute(
        "SELECT id, seen_key, url, old_value, new_value, at, posting_id "
        "FROM state_events WHERE field='status' ORDER BY seen_key, at, id"
    ).fetchall()
    job_state_rows = conn.execute("SELECT seen_key, url, posting_id FROM job_state").fetchall()
    seen_key_url = {r["seen_key"]: r["url"] for r in job_state_rows}
    seen_key_resolved_pid = _seen_key_posting_ids(conn)

    by_seen_key: dict = {}
    for r in event_rows:
        by_seen_key.setdefault(r["seen_key"], []).append(r)

    identity_groups: dict = {}
    for seen_key, rows in by_seen_key.items():
        pid = seen_key_resolved_pid.get(seen_key)
        identity = ("pid", pid) if pid else ("sk", seen_key)
        identity_groups.setdefault(identity, []).extend(rows)

    out = []
    for identity, rows in identity_groups.items():
        rows = sorted(rows, key=lambda r: (r["at"], r["id"]))
        applied_ats = [r["at"] for r in rows if r["new_value"] == "Applied"]
        if not applied_ats:
            continue
        responded = any(
            r["old_value"] == "Applied" and r["new_value"] in _RESPONSE_STAGES for r in rows
        )
        posting_id = identity[1] if identity[0] == "pid" else None
        url = next((r["url"] for r in rows if r["url"]), None) or seen_key_url.get(
            rows[0]["seen_key"]
        )
        _source, _category, match_band, competition_band, _family = (
            outcome_analytics._attribute_dimensions(conn, posting_id, url, profile)
        )
        out.append(
            {
                "identity_key": f"{identity[0]}:{identity[1]}",
                "posting_id": posting_id,
                "applied_at": min(applied_ats, key=_parse),
                "responded": responded,
                "match_band": match_band,
                "competition_band": competition_band,
            }
        )
    return out


def _naive(moment):
    """ISO strings reach this module from two places: `state_events.at`, which
    is local-naive by construction, and the caller's `now`, which a router may
    well build in UTC and stamp with an offset. Comparing the two directly
    raises TypeError, so the offset is dropped for the maturity comparison --
    the alternative (localizing naive event timestamps) would be inventing a
    timezone the database never recorded. The window is 21 DAYS; an hours-scale
    offset cannot move a row across it."""
    return moment.replace(tzinfo=None) if moment.tzinfo is not None else moment


def _is_mature(applied_at: str, now: str) -> bool:
    """True when the application has been outstanding at least
    `RESPONSE_MATURITY_DAYS` -- long enough for "no response" to mean something
    other than "not yet". Boundary is inclusive (`>=`): a row applied exactly
    the window ago is mature."""
    elapsed = (_naive(_parse(now)) - _naive(_parse(applied_at))).total_seconds()
    return elapsed >= RESPONSE_MATURITY_DAYS * 86400.0


def _time_ordered_split(rows: list) -> tuple[list, list]:
    """Oldest `_TRAIN_FRACTION` by applied-at into train, newest remainder into
    held-out. Sorted by (applied timestamp, identity_key) so the result is a
    pure function of the row SET, not of the order rows arrived in -- two rows
    applied at the same instant still land in a fixed order."""
    ordered = sorted(rows, key=lambda r: (_parse(r["applied_at"]), r.get("identity_key") or ""))
    n_train = int(len(ordered) * _TRAIN_FRACTION)
    return ordered[:n_train], ordered[n_train:]


def _feature_vectors(conn: sqlite3.Connection, posting_ids) -> dict:
    """posting_id -> ordered feature vector, for postings whose CURRENT score
    carries a parseable `features_json`. Postings with no score, no features,
    or unparseable features are simply absent -- the model is trained on the
    rows it genuinely has features for, and the count it used is reported.

    The current score is resolved by `outcomes._current_score` (the same
    superseded_at/created_at/score_version_id precedence W-5.2 and W-5.3 use);
    the features themselves are fetched in IN-list chunks, mirroring
    `outcome_analytics._feature_presence`."""
    score_id_by_posting: dict = {}
    for posting_id in posting_ids:
        if posting_id is None:
            continue
        score_row = outcomes._current_score(conn, posting_id)
        if score_row is not None and score_row["score_version_id"]:
            score_id_by_posting[posting_id] = score_row["score_version_id"]

    features_by_score: dict = {}
    for chunk in _chunks(score_id_by_posting.values()):
        rows = conn.execute(
            "SELECT score_version_id, features_json FROM score_versions "
            f"WHERE score_version_id IN ({_in_clause(len(chunk))})",
            list(chunk),
        ).fetchall()
        for r in rows:
            if not r["features_json"]:
                continue
            try:
                parsed = json.loads(r["features_json"])
            except (TypeError, ValueError):
                continue
            if not isinstance(parsed, dict):
                continue
            features_by_score[r["score_version_id"]] = parsed

    vectors = {}
    for posting_id, score_version_id in score_id_by_posting.items():
        parsed = features_by_score.get(score_version_id)
        if parsed is None:
            continue
        vectors[posting_id] = [
            _as_float((parsed.get(group) or {}).get(name)) for group, name in _FEATURE_NAMES
        ]
    return vectors


def _as_float(value) -> float:
    """A missing or non-numeric feature reads as 0.0 -- the scorer's own
    convention (an unfired rule contributes nothing), not an imputation."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


# --------------------------------------------------------------------------- #
# model arm: pure-Python logistic regression
# --------------------------------------------------------------------------- #
def _sigmoid(z: float) -> float:
    """Clamped so a large margin cannot raise OverflowError -- at |z| = 60 the
    output is already 1e-26 from the asymptote, far below anything a Brier
    score notices."""
    if z < -60.0:
        z = -60.0
    elif z > 60.0:
        z = 60.0
    return 1.0 / (1.0 + math.exp(-z))


def _standardizer(train_x: list) -> tuple[list, list]:
    """Per-column (mean, scale) from the TRAIN rows only -- held-out rows must
    not influence the transform any more than they influence the weights. A
    column with no variance gets scale 1.0, leaving it at a constant 0 offset
    rather than dividing by zero."""
    n_cols = len(_FEATURE_NAMES)
    n = len(train_x)
    means = [sum(row[j] for row in train_x) / n for j in range(n_cols)]
    scales = []
    for j in range(n_cols):
        var = sum((row[j] - means[j]) ** 2 for row in train_x) / n
        scales.append(math.sqrt(var) if var > 0.0 else 1.0)
    return means, scales


def _apply_standardizer(x: list, means: list, scales: list) -> list:
    return [(x[j] - means[j]) / scales[j] for j in range(len(x))]


def _fit_logistic(x_rows: list, y: list) -> tuple[list, float]:
    """Batch gradient descent on mean log-loss. Weights start at ZERO (not at
    a seeded random draw), the iteration count is FIXED (no tolerance-based
    early stop, whose trigger point can differ between equivalent runs), and
    the gradient is summed in list order. Same inputs, same weights."""
    n_cols = len(_FEATURE_NAMES)
    n = len(x_rows)
    weights = [0.0] * n_cols
    bias = 0.0
    for _ in range(_ITERATIONS):
        grad = [0.0] * n_cols
        grad_bias = 0.0
        for row, label in zip(x_rows, y):
            z = bias
            for j in range(n_cols):
                z += weights[j] * row[j]
            err = _sigmoid(z) - label
            grad_bias += err
            for j in range(n_cols):
                grad[j] += err * row[j]
        bias -= _LEARNING_RATE * grad_bias / n
        for j in range(n_cols):
            weights[j] -= _LEARNING_RATE * grad[j] / n
    return weights, bias


def _predict(row: list, weights: list, bias: float) -> float:
    z = bias
    for j in range(len(weights)):
        z += weights[j] * row[j]
    return _sigmoid(z)


def _brier(predictions: list, labels: list) -> float:
    return sum((p - y) ** 2 for p, y in zip(predictions, labels)) / len(labels)


def _admits(brier_model: float, brier_empirical: float, brier_intercept: float) -> bool:
    """The admission rule, named so it can be pinned on its own: the model is
    admitted only when it STRICTLY beats BOTH floors on the same held-out rows.

    Beating the band rates alone is not enough. The cell baseline can be
    pathologically bad (a cell that responded 0/85 in train predicts 0.0, so a
    held-out responder scores 1.0 against it), and "less catastrophic than a
    catastrophe" is not evidence of signal. The intercept -- one number, the
    train-set base rate -- is the floor that says the features earned their
    keep. Ties go to the simpler predictor in both comparisons: a tie means the
    scorer's features added nothing the simpler thing did not already carry."""
    return brier_model < brier_empirical and brier_model < brier_intercept


def _train_base_rate(train_rows: list) -> float:
    return (
        sum(1 for r in train_rows if r["responded"]) / len(train_rows) if train_rows else 0.0
    )


def _baseline_predictor(train_rows: list):
    """The first thing the model has to beat: the empirical match x competition
    cell rate, fitted on TRAIN rows only. A held-out row whose cell never
    appeared in train falls back to the overall train response rate -- the
    honest "I have no cell-specific evidence" answer, not a zero."""
    overall = _train_base_rate(train_rows)
    by_cell: dict = {}
    for key, rows in _group(train_rows, _cell_key).items():
        by_cell[key] = sum(1 for r in rows if r["responded"]) / len(rows)

    def predict(row) -> float:
        return by_cell.get(_cell_key(row), overall)

    return predict


def _intercept_predictor(train_rows: list):
    """The second floor: the intercept-only model. One number for everybody --
    the train-set overall response rate -- carrying no band information and no
    feature information at all. Fitted on TRAIN rows only, for the same reason
    the cell baseline is: a predictor that has seen the held-out labels is not
    a baseline, it is a leak."""
    overall = _train_base_rate(train_rows)

    def predict(_row) -> float:
        return overall

    return predict


def _model_accounting(
    *, n_considered: int, n_immature: int, n_featured: int, n_dropped: int
) -> dict:
    """The row-accounting block every model section carries, in EVERY state
    including the not-attempted ones. A reader must be able to follow the
    population from "applications that passed the gate" down to "rows the fit
    actually saw" without subtracting numbers that were never published:
    `n_applied_considered - n_immature - n_dropped_no_features == n_featured`,
    and `n_featured == n_train + n_heldout` once the split runs."""
    return {
        "n_applied_considered": n_considered,
        "n_immature": n_immature,
        "n_featured": n_featured,
        "n_dropped_no_features": n_dropped,
        "n_features": len(_FEATURE_NAMES),
        "maturity_days": RESPONSE_MATURITY_DAYS,
    }


def _model_section(conn: sqlite3.Connection, rows: list, now: str | None) -> dict:
    """Fit, evaluate, and adjudicate the model arm. The verdict is reported
    whichever way it goes: a model that LOSES to the band rates is evidence
    that the bands are already as good as the features, and that is worth
    saying out loud."""
    if now is None:
        # Not a degraded attempt -- a refused one. Maturity is measured against
        # a reference moment, and with no reference every row's "no response"
        # label is of unknown age. Reading the clock here to supply one is
        # exactly what this module does not do.
        return {
            "attempted": False,
            "reason": "no-now",
            "n_train": 0,
            "n_heldout": 0,
            **_model_accounting(
                n_considered=len(rows), n_immature=0, n_featured=0, n_dropped=0
            ),
        }

    mature = [r for r in rows if _is_mature(r["applied_at"], now)]
    n_immature = len(rows) - len(mature)

    vectors = _feature_vectors(conn, [r["posting_id"] for r in mature])
    featured = [r for r in mature if r["posting_id"] in vectors]
    accounting = _model_accounting(
        n_considered=len(rows),
        n_immature=n_immature,
        n_featured=len(featured),
        n_dropped=len(mature) - len(featured),
    )
    if not featured:
        return {
            "attempted": False,
            "reason": "no-featured-rows",
            "n_train": 0,
            "n_heldout": 0,
            **accounting,
        }

    train_rows, heldout_rows = _time_ordered_split(featured)
    # Held-out first, then train: with a fixed 70/30 split the held-out floor
    # is the one a SMALL dataset trips, and the train floor the one a
    # medium-sized dataset trips. Checked the other way round, "insufficient-
    # holdout" would be unreachable and the reason string would always name
    # the coarser problem.
    if len(heldout_rows) < _MIN_HELDOUT:
        return {
            "attempted": False,
            "reason": "insufficient-holdout",
            "n_train": len(train_rows),
            "n_heldout": len(heldout_rows),
            "min_heldout": _MIN_HELDOUT,
            **accounting,
        }
    if len(train_rows) < _MIN_TRAIN:
        return {
            "attempted": False,
            "reason": "insufficient-train",
            "n_train": len(train_rows),
            "n_heldout": len(heldout_rows),
            "min_train": _MIN_TRAIN,
            **accounting,
        }

    train_x_raw = [vectors[r["posting_id"]] for r in train_rows]
    means, scales = _standardizer(train_x_raw)
    train_x = [_apply_standardizer(x, means, scales) for x in train_x_raw]
    train_y = [1.0 if r["responded"] else 0.0 for r in train_rows]
    weights, bias = _fit_logistic(train_x, train_y)

    heldout_y = [1.0 if r["responded"] else 0.0 for r in heldout_rows]
    model_preds = [
        _predict(_apply_standardizer(vectors[r["posting_id"]], means, scales), weights, bias)
        for r in heldout_rows
    ]
    baseline = _baseline_predictor(train_rows)
    baseline_preds = [baseline(r) for r in heldout_rows]
    intercept = _intercept_predictor(train_rows)
    intercept_preds = [intercept(r) for r in heldout_rows]

    brier_model = _brier(model_preds, heldout_y)
    brier_empirical = _brier(baseline_preds, heldout_y)
    brier_intercept = _brier(intercept_preds, heldout_y)
    return {
        "attempted": True,
        "admitted": _admits(brier_model, brier_empirical, brier_intercept),
        "brier_model": brier_model,
        "brier_empirical": brier_empirical,
        "brier_intercept": brier_intercept,
        "n_train": len(train_rows),
        "n_heldout": len(heldout_rows),
        "iterations": _ITERATIONS,
        "learning_rate": _LEARNING_RATE,
        **accounting,
    }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def calibration_report(
    conn: sqlite3.Connection,
    *,
    min_applications: int = 50,
    min_responses: int = 10,
    now: str | None = None,
) -> dict:
    """Whether outcome evidence justifies calibrated probabilities, and what
    they are when it does.

    `min_applications` / `min_responses` are the gate thresholds; the gate is
    OPEN only when both are met (`>=`). While gated the returned payload has no
    `empirical` and no `model` key at all -- see this module's docstring on why
    absence is structural rather than nulled.

    `now` is injected, never read from the clock. It is echoed as
    `generated_at` AND it is the reference moment for the model arm's
    response-maturity window -- the one place a "current moment" legitimately
    enters, because deciding whether a negative label is real requires knowing
    how old it is. Nothing in the gate or the empirical section touches it.
    With `now=None` the empirical section is unaffected and the model arm is
    not attempted (`reason: "no-now"`).
    """
    profile = outcomes._load_profile()
    identities = _application_identities(conn, profile)
    n_applications = len(identities)
    n_responses = sum(1 for i in identities if i["responded"])
    gated = not (n_applications >= min_applications and n_responses >= min_responses)

    report: dict = {
        "generated_at": now,
        "min_sample": _MIN_SAMPLE,
        "gate": {
            "gated": gated,
            "n_applications": n_applications,
            "n_responses": n_responses,
            "thresholds": {
                "min_applications": min_applications,
                "min_responses": min_responses,
            },
        },
        "active": "gated",
        "heuristic_bands": _heuristic_bands(identities),
    }
    if gated:
        return report

    report["empirical"] = _empirical(identities)
    model = _model_section(conn, _applied_rows(conn, profile), now)
    report["model"] = model
    report["active"] = "model" if model.get("admitted") else "empirical"
    return report
