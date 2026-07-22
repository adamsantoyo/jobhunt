# Phase 0 Recon: `job_state` identity call-site map

Exhaustive scan of every code path (backend, frontend, tests) that reads or writes `job_state.url`, `job_state.seen_key`, `needs_review`, `review_reason`, `review_dismissed`, the `orphaned:` URL convention, `/api/review`, `/api/review/reconcile`. Grouped by W3 refactor strategy.

---

## MUST-CHANGE (W3 refactors the logic itself)

### Schema & DDL

**`webapp/backend/db.py:26,30-32`** — `job_state` table definition
- Columns: `url TEXT PRIMARY KEY`, `seen_key TEXT NOT NULL`, `needs_review`, `review_reason`, `review_dismissed`
- **W3 change**: `seen_key` becomes PRIMARY KEY; url → display column; review columns dropped from schema
- **Note**: `_ensure_column()` at :74 is migration safety; DDL at :13-50 defines baseline schema

**`webapp/backend/db.py:21,33`** — Indexes on `seen_key`
- `CREATE INDEX idx_jobs_seen_key ON jobs(seen_key)` (jobs table)
- `CREATE INDEX idx_state_seen_key ON job_state(seen_key)` (job_state table)
- **W3 change**: idx_state_seen_key becomes part of collision detection during migration; idx_jobs_seen_key unchanged

---

### Ingest healing logic (Pass A & B + orphan parking)

**`webapp/backend/ingest.py:63-68`** — `_surrogate_url(row_url, seen_key)` function
- Returns `withheld:<seen_key>` for non-http(s) rows (stable surrogate PK)
- **W3 change**: This function remains; used during jobs upsert only, not state rewrite

**`webapp/backend/ingest.py:75-84`** — `_free_orphan_url(cur, seen_key)` function
- Generates free `orphaned:<seen_key>` (or `#2`, `#3`, etc. on collision) to park detached state rows
- **W3 change**: MUST DELETE — W3 ingest simplification removes orphan parking entirely; state rows move directly to winner via seen_key match

**`webapp/backend/ingest.py:208-277`** — Healing logic (Pass A: identity check, Pass B: seen-key heal)
- Lines 208-209: Load all state rows with url, seen_key, needs_review, review_dismissed
- Lines 216-237: **Pass A** — for each state row:
  - If url not in present_urls → mark for heal
  - If url present but seen_key mismatch → detach to orphan surrogate (line 231), update review_reason (line 233)
  - If url present + seen_key match → clear review flags (lines 223-227)
- Lines 243-277: **Pass B** — for detached rows:
  - Single candidate + not taken → re-anchor to candidate (lines 247-252), clear review flags
  - No candidates → job disappeared; only clear flags if was_flagged (lines 257-261)
  - ≥2 candidates or candidate taken → ambiguous; set needs_review=1 + review_reason (lines 273-276) UNLESS review_dismissed (lines 265-271) then keep reason fresh but don't re-flag
- **W3 change**: DELETE both passes entirely and orphan parking. Replace with single post-upsert step: for each state row whose seen_key has a present jobs row, update state.url to that row's url (deterministic refresh). No collision detection, no review flagging, no orphan parking.

---

### User state mutation (needs_review clearing)

**`webapp/backend/routers/state.py:61-95`** — `_apply_state(conn, url, changes)` function
- UPSERT that touches only supplied fields; every user edit clears needs_review
- Line 77: `seen_key=_resolve_seen_key(conn, url) or ""` — derives seen_key from jobs cache or existing state
- Lines 83: `needs_review=0, review_reason=NULL` in all UPDATE branches
- Line 83: `review_dismissed` never touched by user PATCH (write-only on reconcile or dismiss action)
- **W3 change**: `seen_key` resolution logic becomes obsolete (state.url → jobs.seen_key lookup during resolution phase); dropping review columns means no clearing needed; `review_dismissed` write becomes no-op

---

### Review list endpoint (flagged state rows)

**`webapp/backend/routers/state.py:189-205`** — `GET /api/review` endpoint
- Line 191: `SELECT * FROM job_state WHERE needs_review=1`
- Lines 194-204: For each flagged row, fetch matching job (via url or synthed from state), find live candidates via seen_key
- **W3 change**: Endpoint becomes `→ always return []` (API-compat shim); query is meaningless once review columns are dropped

---

### Reconcile endpoint (attach orphaned state to chosen candidate)

**`webapp/backend/routers/state.py:208-234`** — `POST /api/review/reconcile` endpoint
- Guarded UPDATE: move state from orphaned url to user-chosen target job
- Line 220: Rewrite `url`, update `seen_key` to target's, clear review flags (needs_review=0, review_reason=NULL, review_dismissed=0)
- **W3 change**: Endpoint becomes `→ 410 Gone` (API-compat shim); no more orphaned rows to reconcile in W3 model

---

### DTO models with review fields

**`webapp/backend/models.py:39-50`** — `JobState` DTO class
- Fields: `needs_review: bool`, `review_reason: Optional[str]`
- **W3 change**: Keep both fields as constants (needs_review=False, review_reason=None) in DTO so frontend doesn't break; return them always

**`webapp/backend/models.py:116-127`** — `StatePatch` request DTO
- Field: `review_dismissed: Optional[bool]` (write-only, never echoed)
- **W3 change**: Keep field for compat; becomes no-op when applied

**`webapp/backend/models.py:153-170`** — `_state_from_row()` helper
- Lines 167-168: Read `needs_review` and `review_reason` from row
- **W3 change**: Return hardcoded constants instead of reading from row

**`webapp/backend/models.py:176-178`** — `JOB_STATE_JOIN_COLS` SQL fragment
- Includes `s.needs_review, s.review_reason`
- **W3 change**: Keep in query for now (read-only), but values are always 0/NULL; eventually drop from SELECT

---

### Analytics with review state (follow-ups, funnel)

**`webapp/backend/routers/analytics.py:37,43,96-101`** — Analytics queries
- Line 37: LEFT JOIN on `j.url = s.url` (url-based join)
- Line 43: `SELECT s.status ... FROM job_state s LEFT JOIN jobs j ON s.url = j.url`
- Lines 96-101: Count follow-ups by follow_up_date range
- **W3 change**: Join becomes `j.seen_key = s.seen_key` (seen_key-based); follow-up queries unchanged

---

### Test ingest scenarios (review flagging)

**`webapp/backend/tests/test_ingest.py:73-79,133-200,223-230,344-405,432-495,556-562`** — Comprehensive test suite exercising healing logic
- Tests verify: ambiguous rewrites flagged (needs_review=1), URL recycling to different role, dismissal is durable, picks seeding, status migration, applied_date backfill
- **W3 change**: UPDATE all tests to expect NEW behavior — no needs_review flagging, no orphan URLs, no reconcile; instead verify deterministic url refresh, zero state loss, seen_key-based anchoring. Preserve test *scenarios* (repost, recycle, disappear, collision) but assert new outcomes.

---

## API-COMPAT-SHIM (W3 returns constants; logic deleted)

### Frontend request DTO

**`webapp/frontend/src/api/types.ts:141-152`** — `StatePatch` interface
- Field: `review_dismissed?: boolean`
- **W3 change**: Keep field; becomes no-op in backend

---

### Frontend response DTOs

**`webapp/frontend/src/api/types.ts:3-15`** — `JobState` interface
- Fields: `needs_review: boolean`, `review_reason: string | null`
- **W3 change**: Keep both fields; backend always returns needs_review=false, review_reason=null (constants)

**`webapp/frontend/src/api/types.ts:17-20`** — `JobLight` interface
- Field: `seen_key: string` (from jobs cache, never mutated in W3)
- **W3 change**: Unchanged; read-only field

**`webapp/frontend/src/api/types.ts:154-157`** — `ReviewItem` interface
- Job + candidates list
- **W3 change**: Unused once `/api/review` returns []; can leave as-is for compat

---

### Backend response serialization

**`webapp/backend/models.py:195-223`** — `job_light_from_row()` helper
- Line 199: Includes `seen_key=row["seen_key"]`
- Line 221: Calls `_state_from_row()` which reads needs_review, review_reason
- **W3 change**: seen_key unchanged; _state_from_row() becomes constant-return

---

### API client bindings

**`webapp/frontend/src/api/client.ts:82,83-84`** — API endpoints
- Line 82: `getReview: () => request<ReviewItem[]>("/api/review")`
- Lines 83-84: `reconcile: (body) => request<JobState>("/api/review/reconcile", { method: "POST", ... })`
- **W3 change**: getReview() keeps working but always returns [] (no-op fetch); reconcile() returns 410 Gone

---

### Query cache & optimistic updates

**`webapp/frontend/src/store/queries.ts:34-56`** — Query hooks & cache merging
- Lines 43-44: `EMPTY_STATE` defaults: `needs_review: false, review_reason: null`
- Lines 50-55: `mergePatch()` function:
  - Strips `review_dismissed` from patch before merging (write-only, never stored)
  - Sets `needs_review: false` after any user edit
- **W3 change**: Unchanged; semantics remain identical (needs_review always false, review_dismissed no-op)

---

## FRONTEND-ONLY (leave alone; no W3 changes)

### Display of flagged review items

**`webapp/frontend/src/components/JobDetailDrawer.tsx:165-167`** — Show warning badge
- Conditional: `{job.state?.needs_review && (...)}`
- Display: `⚠ Needs review: <review_reason>` or just `⚠ Needs review`
- **W3 change**: None — display remains, but badge never shows (needs_review always false)

---

### Review page & reconciliation UI

**`webapp/frontend/src/pages/Review.tsx:1-136`** — Full review flow page
- Lists flagged job_state rows (line 191 query), shows candidates via seen_key match, offers Attach or Dismiss
- Dismiss action (line 50): sends `{ review_dismissed: true }` patch
- Attach action (line 129): POST reconcile with from/to url_b64 pairs
- **W3 change**: None — page stays, but list is always empty (query returns [])

---

### Review query hook

**`webapp/frontend/src/store/queries.ts`** — `useReview()` hook and mutations
- Queries `/api/review`, caches ReviewItem[], invalidates on reconcile/dismiss mutations
- **W3 change**: None — hooks unchanged; queries return [] (empty list)

---

## Summary: call-site counts

| Category | Count | Note |
|----------|-------|------|
| **Must-change** | 9 | Schema, healing (Pass A/B), orphan parking, _apply_state, endpoints, test scenarios |
| **API-compat-shim** | 7 | DTOs (needs_review, review_reason as constants), endpoints return [] or 410, frontend types unchanged |
| **Frontend-only** | 3 | Display badge, Review page, query hook (all become no-ops but stay in place) |

---

## Orphaned URL convention (`orphaned:<seen_key>[#N]`)

**Locations**:
- `ingest.py:76-84` — `_free_orphan_url()` generates; used in Pass A detach (line 231)
- `test_ingest.py` — multiple tests verify orphan generation and healing (e.g., lines 357, 401)

**W3 change**: Delete orphan generation entirely. State rows no longer park on orphaned URLs; they anchor directly to seen_key matches or stay dormant (no review flag). Remove all `orphaned:` handling from ingest, remove _free_orphan_url function, simplify Pass B logic to single deterministic refresh step.

---

## Seen-key usage (identity anchor for state rows)

**Read path** (no W3 change needed):
- `ingest.py:124,152,193` — compute seen_key from CSV rows (identity.py function)
- `ingest.py:149-156` — build seen_by_key dict (present url → seen_key map)
- `ingest.py:208,219,236` — read existing state.seen_key for healing decisions
- `routers/state.py:54-58` — `_resolve_seen_key()` fetches from state or jobs cache
- `models.py:176-180` — include in JOIN and SELECT projections
- `routers/analytics.py:43` — join on seen_key (after W3: changes from url)

**Write path** (W3 change at ingest.py only):
- `ingest.py:164,248,299` — INSERT/UPDATE state.seen_key during healing
- **W3 change** at line 248: after collision detection & winner selection, set winning state.seen_key to the present row's computed seen_key (deterministic; no ambiguity)

---

## job_state.url usage (PK before W3; display column after)

**Current PK**:
- `db.py:25` — `url TEXT PRIMARY KEY`
- `ingest.py:81,208,233,239,280,299` — WHERE url=? and INSERT/UPDATE
- `routers/state.py:36,54,90,108,191,219,224,228` — route resolution, queries, upserts, reconcile
- `routers/analytics.py:43` — LEFT JOIN on url (becomes seen_key join in W3)
- `models.py:180,192` — LEFT JOIN on url (becomes seen_key join in W3)

**W3 change**:
- Schema: url becomes `TEXT NOT NULL` (non-PK display column)
- Primary key: `seen_key TEXT PRIMARY KEY`
- **Must update**:
  - `db.py:25-26` — schema change
  - `ingest.py:233,248,299` — state.url becomes display update only (not PK mutation)
  - `routers/state.py:90` — INSERT uses new PK on conflict
  - `routers/analytics.py:43` — JOIN changes to seen_key
  - `models.py:180,192` — JOIN changes to seen_key
  - All WHERE url=? queries → WHERE seen_key=? or dual-lookup (URL → seen_key → state row)
