"""The source-adapter contract: the single source of truth for Phase 2.

Every protocol adapter (Greenhouse, Lever, Ashby, SmartRecruiters, Workable,
Recruitee, Workday, Eightfold/Microsoft, Amazon, iCIMS, Phenom, Jibe/Costco,
Built In, YC, JobSpy, manual import) and the Phase 2.3 scheduler code against
the types in this module. Nothing here touches the network, the database, the
clock-as-a-side-effect, or any third-party package: it is pure typed data plus
two Protocols.

Division of responsibility (non-negotiable, the scheduler depends on it):

  The ADAPTER                             The SCHEDULER
  ------------------------------------    ------------------------------------
  expands config into targets (`plan`)     decides which targets run, and when
  issues transport requests                owns the transport, its pools, and
                                           per-host / global concurrency
  parses payloads into records             persists records, dedupes, batches
  raises classified errors                 retries (at most once, transient
                                           only, with jitter) and gives up
  yields records as it finds them          enforces the one deadline
  reports progress via checkpoints         persists and replays checkpoints

Invariants an adapter MUST honour. These are load-bearing; the scheduler's
timing guarantees and Phase 2.4's absence semantics are unsound if violated.

1. ADAPTERS NEVER RETRY. One request failure is one raised error. The
   scheduler classifies it and decides whether the single permitted retry is
   spent on it.
2. ADAPTERS NEVER SLEEP. No `time.sleep`, no `asyncio.sleep` for politeness or
   backoff. Inter-request pacing is declared on the descriptor
   (`min_request_interval_seconds`) and applied by the scheduler-owned
   transport. A sleeping adapter burns the run's deadline budget invisibly.
3. ADAPTERS NEVER SWALLOW ERRORS. `scraper.py` returns `[]` on a non-200, which
   is indistinguishable from "this board genuinely has no jobs" and, under
   Phase 2.4, would mark every posting of a healthy company absent. Under this
   contract an adapter that cannot enumerate its target raises; yielding zero
   records is a positive assertion that the target is empty.
4. YIELD ORDER IS NOT SIGNIFICANT. The writer keys on identity, not arrival
   order. Adapters must not rely on the consumer observing any ordering.
5. EMITTING A RECORD TWICE MUST BE SAFE. Checkpoints may be replayed after a
   crash or a retry, and search-term fan-out overlaps heavily. The writer
   dedupes on identity, so re-emission is expected, not an error. Adapters may
   suppress obvious in-run duplicates as an efficiency measure, never as a
   correctness measure.
6. RECORDS ARE STREAMED, NOT BATCHED. `fetch` is an async generator so new jobs
   reach the UI before the run finishes (Success Contract). An adapter that
   accumulates everything and yields at the end technically conforms but
   forfeits that guarantee; page-at-a-time yielding is required for paginated
   sources.
7. PARSING IS PURE. Every adapter exposes module-level `parse_*` functions that
   take bytes/text/JSON plus a `SourceTarget` and return records. `fetch` is a
   thin transport shell over them, so frozen fixtures exercise the parser with
   zero network in CI.
8. NO WALL-CLOCK BRANCHING. Adapters must not consult the deadline to decide
   whether to keep going; `ctx.remaining_seconds()` exists for logging and for
   a cooperative early stop at a page boundary, never for correctness.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
import unicodedata
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar, Protocol, runtime_checkable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

__all__ = [
    "CANONICAL_HASH_FIELDS",
    "CHECKPOINT_VERSION",
    "Checkpoint",
    "ConfigError",
    "Disposition",
    "ExecutionMode",
    "FetchContext",
    "HttpRequest",
    "HttpResponse",
    "IdentityClaim",
    "InboundPayload",
    "InventoryScope",
    "NormalizedPosting",
    "PayloadError",
    "PermanentSourceError",
    "RunKind",
    "SourceAdapter",
    "SourceCategory",
    "SourceConfig",
    "SourceDescriptor",
    "SourceError",
    "SourceTarget",
    "TransientSourceError",
    "Transport",
    "TransportKind",
    "check_status",
    "classify_status",
    "collapse_whitespace",
    "normalize_date",
    "normalize_text",
    "normalize_url",
]

JSONValue = Any
"""Anything `json.dumps` accepts. Deliberately loose: checkpoint cursors and
adapter `extra` payloads are opaque to the scheduler, which only round-trips
them through `source_runs.checkpoint_json` / `posting_versions.payload_json`."""


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class Disposition(StrEnum):
    """How the scheduler should treat an outcome."""

    SUCCESS = "success"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


class SourceError(Exception):
    """Base class for every failure an adapter is allowed to surface.

    The scheduler branches on `disposition`, never on the exception message, so
    that the "at most one classified transient retry" rule is decided by the
    raiser (which knows what happened) rather than by string matching.
    `to_json_dict()` is what lands in `source_runs.error_json`.
    """

    disposition: ClassVar[Disposition] = Disposition.PERMANENT

    def __init__(
        self,
        message: str,
        *,
        source_key: str = "",
        instance_key: str = "",
        status: int | None = None,
        url: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.source_key = source_key
        self.instance_key = instance_key
        self.status = status
        self.url = url

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "type": type(self).__name__,
            "disposition": str(self.disposition),
            "message": self.message,
            "source_key": self.source_key,
            "instance_key": self.instance_key,
            "status": self.status,
            "url": self.url,
        }


class TransientSourceError(SourceError):
    """Worth exactly one retry: timeouts, connection resets, 429, 5xx."""

    disposition: ClassVar[Disposition] = Disposition.TRANSIENT


class PermanentSourceError(SourceError):
    """Retrying cannot help: 404 board, 401/403, malformed payload, bad config."""

    disposition: ClassVar[Disposition] = Disposition.PERMANENT


class PayloadError(PermanentSourceError):
    """The transport succeeded but the body is not what this adapter parses.

    Raised for a malformed envelope (not JSON, wrong shape at the top level),
    which means the source changed its API and the adapter is now broken. An
    individual unusable *item* inside a well-formed envelope is skipped instead
    of raised: one bad row must not blank a whole board.
    """


class ConfigError(PermanentSourceError):
    """`plan()` was handed configuration it cannot turn into a target."""


# --------------------------------------------------------------------------- #
# Enumerations
# --------------------------------------------------------------------------- #
class SourceCategory(StrEnum):
    """Failure domain and freshness expectation, not merely a label.

    DIRECT and AGGREGATOR are separate failure domains by roadmap decision: an
    aggregator outage must never degrade direct-source results or block the UI.
    """

    DIRECT = "direct"
    AGGREGATOR = "aggregator"
    STARTUP_BOARD = "startup-board"
    MANUAL = "manual"


class RunKind(StrEnum):
    """Phase 2.5 run kinds. `SourceDescriptor.run_kinds` declares membership."""

    DAILY = "daily"
    FULL_DIRECT = "full-direct"
    AGGREGATORS = "aggregators"
    LLM_REVIEW = "llm-review"
    MANUAL_IMPORT = "manual-import"


class ExecutionMode(StrEnum):
    """Where the adapter's `fetch` body is allowed to run.

    ASYNC_INPROCESS: cooperative async in the scheduler's event loop. The
        default, and the only mode that may hold the loop.
    SUBPROCESS: the work blocks (JobSpy calls into pandas and blocks for
        minutes). The ADAPTER ITSELF must fork an isolated cancellable
        subprocess that streams `NormalizedPosting.to_json_dict()` lines back
        over stdout; the parent re-hydrates them with `from_json_dict` and
        yields them. This is why the record type is required to be JSON
        round-trippable. The scheduler does NOT provide this isolation — the
        mode is a declaration the adapter must honor, not a service it
        receives (deliberate Phase 2 decision, 2026-08-04): an adapter whose
        fetch body blocks in-process stalls every other source's deadline
        regardless of what it declares. Any new SUBPROCESS adapter must ship a
        scheduler-level test proving its child is cancellable and reaped, as
        JobSpy's does (grep for
        test_the_real_jobspy_subprocess_adapter_is_cancellable).
    PUSH: no transport at all. Records arrive from outside the scheduler
        (manual MCP import) as `InboundPayload`s on the context.
    """

    ASYNC_INPROCESS = "async-inprocess"
    SUBPROCESS = "subprocess"
    PUSH = "push"


class TransportKind(StrEnum):
    """Whether the scheduler must hand this adapter an HTTP transport."""

    HTTP = "http"
    NONE = "none"


class InventoryScope(StrEnum):
    """Whether a successful run licenses marking unseen postings absent.

    COMPLETE: the target enumerates its entire inventory (a Greenhouse board, a
        Lever account, an iCIMS portal paged to exhaustion). A successful run
        means "these are all of them", so Phase 2.4 may mark the rest absent.
    PARTIAL: the target is a keyword/geography search (Workday, Eightfold,
        Amazon, Built In, JobSpy) or an out-of-band drop (manual import). Not
        seeing a posting proves nothing — the query simply may not have matched
        it — so absence must NEVER be inferred from a successful run.

    Getting this wrong silently deletes live jobs, which is why it is on the
    target rather than left to the scheduler to guess from the source key.
    """

    COMPLETE = "complete"
    PARTIAL = "partial"


def classify_status(status: int) -> Disposition:
    """Map an HTTP status onto a retry decision. One table, no per-adapter drift."""
    if 200 <= status < 300:
        return Disposition.SUCCESS
    if status in (408, 425, 429) or 500 <= status < 600:
        return Disposition.TRANSIENT
    return Disposition.PERMANENT


# --------------------------------------------------------------------------- #
# Normalization primitives
#
# These exist so the Phase 3.1 content hash is computed over one canonical
# spelling of a record. Every adapter routes its raw strings through them, so a
# source that starts emitting non-breaking spaces or a tracking parameter does
# not manufacture a wave of spurious "material changes".
# --------------------------------------------------------------------------- #
_WS_RE = re.compile(r"\s+")
_ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")

#: Query parameters stripped from a URL before it is used as an alias or hashed.
#: Deliberately conservative: dropping a *meaningful* parameter would collapse
#: two distinct postings into one identity, which is far worse than a hash that
#: churns on a tracking tag. Add only params proven to be pure tracking.
TRACKING_PARAMS = frozenset(
    {
        "gclid",
        "fbclid",
        "mc_cid",
        "mc_eid",
        "gh_src",
        "igshid",
        "_ga",
        "_gl",
        "trk",
        "trackingid",
        "ref_src",
    }
)
_TRACKING_PREFIXES = ("utm_",)


def collapse_whitespace(value: str | None) -> str:
    """`"  Support   Engineer\\n"` -> `"Support Engineer"`."""
    if not value:
        return ""
    return _WS_RE.sub(" ", str(value)).strip()


def normalize_text(value: str | None) -> str:
    """Unicode-normalize (NFKC) then collapse whitespace.

    NFKC folds the full-width and non-breaking variants that ATS HTML sprays
    into titles and locations; without it the same posting hashes differently
    depending on which endpoint served it.
    """
    if not value:
        return ""
    return collapse_whitespace(unicodedata.normalize("NFKC", str(value)))


def normalize_url(url: str | None) -> str:
    """Reduce a URL to the stable form used as a URL alias and in the hash.

    Lowercases scheme and host, drops the default port, drops the fragment,
    drops known tracking parameters, sorts the surviving query (ATS endpoints
    reorder them freely), and strips a trailing slash. Non-http(s) values pass
    through trimmed, so surrogate identifiers survive untouched.

    This value is an *alias candidate*, never a global primary key: two sources
    can legitimately point at the same URL, and one posting can have several.
    """
    raw = (url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https"):
        return raw
    host = (parts.hostname or "").lower()
    if parts.port and not (
        (parts.scheme == "http" and parts.port == 80)
        or (parts.scheme == "https" and parts.port == 443)
    ):
        host = f"{host}:{parts.port}"
    kept = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
        and not k.lower().startswith(_TRACKING_PREFIXES)
    ]
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), host, path, urlencode(sorted(kept)), ""))


def normalize_date(value: Any) -> str | None:
    """Coerce a source date to `YYYY-MM-DD`, or `None` when it is not a date.

    Returning `None` rather than the raw string is deliberate. Several sources
    report *relative* recency ("Posted 30+ Days Ago" from Workday, "3 days ago"
    from listing HTML), which would change every day and mint a bogus posting
    version on every run. Only an absolute date is allowed into the hash; keep
    the original in `posted_raw`, which is not hashed.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    m = _ISO_DATE_RE.match(text)
    if not m:
        return None
    year, month, day = (int(g) for g in m.groups())
    if not (1970 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _freeze(mapping: Mapping[str, JSONValue] | None) -> Mapping[str, JSONValue]:
    return MappingProxyType(dict(mapping or {}))


def _stable_digest(payload: JSONValue) -> str:
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Identity
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class IdentityClaim:
    """One precedence-ordered claim about which canonical posting a record is.

    Maps 1:1 onto a `posting_aliases` row (`alias_kind`, `namespace`, `value`,
    `url`, `req_id`). Precedence is the Phase 1 rule, restated here because
    adapters are where the evidence originates:

      rank 0  source-native requisition id, namespaced by source+instance.
              Authoritative. Greenhouse job 4020123 on Anthropic's board is
              that posting, forever.
      rank 1  normalized URL. Conservative secondary evidence only. A URL is
              NEVER globally unique: aggregators mirror it, boards recycle it,
              and one posting routinely has several.

    Adapters produce claims; they never resolve them. Resolution is Phase 3.
    """

    kind: str
    namespace: str
    value: str
    rank: int
    url: str | None = None
    req_id: str | None = None


# --------------------------------------------------------------------------- #
# The record
# --------------------------------------------------------------------------- #
#: Frozen field order of the content hash. Phase 3.1 hashes a record to decide
#: whether to mint a `posting_versions` row, so this order and this membership
#: are a compatibility surface: changing either invalidates every stored hash
#: and re-versions the whole corpus. Adding a field requires a deliberate
#: re-hash migration, not a code tweak.
CANONICAL_HASH_FIELDS: tuple[str, ...] = (
    "source_key",
    "namespace",
    "req_id",
    "url_key",
    "title",
    "company",
    "location",
    "posted_date",
    "salary",
    "remote",
    "description_digest",
)


@dataclass(frozen=True, slots=True)
class NormalizedPosting:
    """One posting as a single source described it, in canonical spelling.

    Supersedes `scraper.rec()`. Every field of that dict survives here; the
    additions exist because the canonical schema needs them:

      `instance_key`   namespaces `req_id` (Workday req numbers are unique per
                       tenant, not globally), and scopes absence marking to the
                       board that was actually enumerated.
      `posted_date` /  splits the absolute date that may be hashed from the raw
      `posted_raw`     string that may not (see `normalize_date`).
      `description`    inline body for the sources that hand one over for free
                       (JobSpy `_desc`, Jibe list responses), so Phase 3.2 can
                       skip a description fetch it does not need.
      `alt_urls`       mirror URLs this single source volunteered (YC
                       `applyUrl` vs `url`). NOT the cross-source `_alts` from
                       `scraper.dedupe` — those are produced by the Phase 3
                       resolver, downstream of this type.
      `extra`          adapter-specific provenance for `payload_json`. Never
                       hashed, never interpreted by the scheduler.

    A record is a claim by one source at one instant. It is not a posting, it
    does not carry a `posting_id`, and it says nothing about scoring — `tier`,
    `odds`, `why`, and `flags` on `posting_versions` are filled by Phase 3.

    Instances are frozen and normalized at construction, so two adapters that
    saw the same posting produce byte-identical canonical forms. Compare with
    `content_hash()`, not `hash()`.
    """

    source_key: str
    title: str
    company: str
    url: str
    instance_key: str = ""
    location: str = ""
    req_id: str | None = None
    posted_date: str | None = None
    posted_raw: str = ""
    salary_text: str = ""
    remote: bool = False
    description: str | None = None
    alt_urls: tuple[str, ...] = ()
    extra: Mapping[str, JSONValue] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        set_ = object.__setattr__
        set_(self, "source_key", (self.source_key or "").strip())
        set_(self, "instance_key", (self.instance_key or "").strip())
        set_(self, "title", normalize_text(self.title))
        set_(self, "company", normalize_text(self.company))
        set_(self, "location", normalize_text(self.location))
        set_(self, "salary_text", normalize_text(self.salary_text))
        set_(self, "posted_raw", collapse_whitespace(self.posted_raw))
        set_(self, "url", (self.url or "").strip())
        set_(self, "remote", bool(self.remote))
        req = (self.req_id or "").strip()
        set_(self, "req_id", req or None)
        posted = normalize_date(self.posted_date)
        set_(self, "posted_date", posted)
        set_(self, "alt_urls", tuple(dict.fromkeys(u.strip() for u in self.alt_urls if u and u.strip())))
        set_(self, "extra", _freeze(self.extra))
        if not self.source_key:
            raise PayloadError("NormalizedPosting requires a source_key")
        if not self.title:
            raise PayloadError(
                "NormalizedPosting requires a title", source_key=self.source_key, url=self.url
            )
        if not self.url:
            raise PayloadError(
                "NormalizedPosting requires a url", source_key=self.source_key, instance_key=self.instance_key
            )
        # A record with no date at all is legitimate: several boards publish none.

    # -- derived ---------------------------------------------------------- #
    @property
    def namespace(self) -> str:
        """Identity namespace for `req_id`: `"greenhouse:anthropic"`.

        Singleton sources (one target for the whole source, e.g. `yc`) leave
        `instance_key` empty and namespace on the source key alone.
        """
        return f"{self.source_key}:{self.instance_key}" if self.instance_key else self.source_key

    @property
    def url_key(self) -> str:
        """The normalized URL used as an alias value and in the hash."""
        return normalize_url(self.url)

    @property
    def description_digest(self) -> str:
        """Whitespace-insensitive digest of the inline description, or `""`.

        The body itself is deliberately kept out of the hash input: it can be
        kilobytes, it is often truncated differently between runs, and hashing
        a digest keeps the canonical form small and reproducible while still
        making a genuine description rewrite a material change.
        """
        if not self.description:
            return ""
        return hashlib.sha256(collapse_whitespace(self.description).encode("utf-8")).hexdigest()

    def canonical_fields(self) -> dict[str, str]:
        """The exact, ordered, all-string input to the content hash.

        Excluded on purpose:
          `posted_raw`  relative recency strings churn daily (see normalize_date).
          `alt_urls`    mirror discovery is a downstream resolver concern; a
                        record's hash must not depend on other sources.
          `extra`       provenance and debugging, not content.
          `description` replaced by its digest, above.
        """
        return {
            "source_key": self.source_key,
            "namespace": self.namespace,
            "req_id": self.req_id or "",
            "url_key": self.url_key,
            "title": self.title,
            "company": self.company,
            "location": self.location,
            "posted_date": self.posted_date or "",
            "salary": self.salary_text,
            "remote": "1" if self.remote else "0",
            "description_digest": self.description_digest,
        }

    def content_hash(self) -> str:
        """`"sha256:<hex>"` over `canonical_fields()`. Phase 3.1's change test."""
        blob = json.dumps(
            self.canonical_fields(), ensure_ascii=False, separators=(",", ":"), sort_keys=False
        )
        return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def identity_claims(self) -> tuple[IdentityClaim, ...]:
        """Precedence-ordered alias evidence. See `IdentityClaim`."""
        claims: list[IdentityClaim] = []
        if self.req_id:
            claims.append(
                IdentityClaim(
                    kind="source_req",
                    namespace=self.namespace,
                    value=self.req_id,
                    rank=0,
                    url=self.url_key or None,
                    req_id=self.req_id,
                )
            )
        if self.url_key:
            claims.append(
                IdentityClaim(
                    kind="url",
                    namespace="url",
                    value=self.url_key,
                    rank=1,
                    url=self.url_key,
                    req_id=self.req_id,
                )
            )
        return tuple(claims)

    # -- serialization ----------------------------------------------------- #
    def to_json_dict(self) -> dict[str, JSONValue]:
        """JSON-safe form. Round-trip fidelity is a hard requirement: it is the
        subprocess wire format for `ExecutionMode.SUBPROCESS` adapters (JobSpy)
        and the on-disk shape of a frozen fixture record."""
        return {
            "source_key": self.source_key,
            "instance_key": self.instance_key,
            "title": self.title,
            "company": self.company,
            "url": self.url,
            "location": self.location,
            "req_id": self.req_id,
            "posted_date": self.posted_date,
            "posted_raw": self.posted_raw,
            "salary_text": self.salary_text,
            "remote": self.remote,
            "description": self.description,
            "alt_urls": list(self.alt_urls),
            "extra": dict(self.extra),
        }

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, JSONValue]) -> NormalizedPosting:
        return cls(
            source_key=payload.get("source_key") or "",
            instance_key=payload.get("instance_key") or "",
            title=payload.get("title") or "",
            company=payload.get("company") or "",
            url=payload.get("url") or "",
            location=payload.get("location") or "",
            req_id=payload.get("req_id"),
            posted_date=payload.get("posted_date"),
            posted_raw=payload.get("posted_raw") or "",
            salary_text=payload.get("salary_text") or "",
            remote=bool(payload.get("remote")),
            description=payload.get("description"),
            alt_urls=tuple(payload.get("alt_urls") or ()),
            extra=payload.get("extra") or {},
        )


# --------------------------------------------------------------------------- #
# Configuration and targets
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SourceConfig:
    """Typed read-only view of `config.json` for `plan()`.

    Mirrors the real file: `profile.search_terms`, `companies.<source_key>`,
    and the free-standing `jobspy` block, which arrives in `options`. Adapters
    receive this instead of the raw dict so a missing key raises `ConfigError`
    from one place rather than `KeyError` from sixteen.
    """

    search_terms: tuple[str, ...] = ()
    companies: Mapping[str, JSONValue] = field(default_factory=dict, hash=False)
    profile: Mapping[str, JSONValue] = field(default_factory=dict, hash=False)
    options: Mapping[str, JSONValue] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        set_ = object.__setattr__
        set_(self, "search_terms", tuple(t for t in self.search_terms if t))
        set_(self, "companies", _freeze(self.companies))
        set_(self, "profile", _freeze(self.profile))
        set_(self, "options", _freeze(self.options))

    @classmethod
    def from_mapping(cls, cfg: Mapping[str, JSONValue]) -> SourceConfig:
        """Build from a parsed `config.json`. Unknown top-level keys other than
        `profile`/`companies` become `options` (this is how the `jobspy` block
        reaches its adapter)."""
        profile = cfg.get("profile") or {}
        companies = cfg.get("companies") or {}
        options = {k: v for k, v in cfg.items() if k not in ("profile", "companies")}
        return cls(
            search_terms=tuple(profile.get("search_terms") or ()),
            companies=companies,
            profile=profile,
            options=options,
        )

    def entries(self, source_key: str) -> Mapping[str, JSONValue]:
        """`companies.<source_key>`, or an empty mapping when unconfigured.

        An unconfigured source is not an error: it plans zero targets and the
        scheduler simply has nothing to run for it.
        """
        entry = self.companies.get(source_key) or {}
        if not isinstance(entry, Mapping):
            raise ConfigError(
                f"companies.{source_key} must be an object, got {type(entry).__name__}",
                source_key=source_key,
            )
        return entry

    def option(self, key: str, default: JSONValue = None) -> JSONValue:
        return self.options.get(key, default)


@dataclass(frozen=True, slots=True)
class SourceTarget:
    """One independently schedulable unit of work: the scheduler's atom.

    A target is NOT a source. `greenhouse` is a source with ~32 targets, one
    per board. The unit is per-instance for two reasons that both bear on
    correctness, not just parallelism:

      * failure isolation — a 404 on one board must not fail, retry, or delay
        the other thirty-one, and each gets its own `source_runs` row with its
        own deadline and attempt count;
      * absence scoping — "completed successfully, therefore everything else is
        absent" is only sound for the exact inventory that was enumerated.

    `source_run_key` is what goes in `source_runs.source`.
    """

    source_key: str
    instance_key: str = ""
    label: str = ""
    params: Mapping[str, JSONValue] = field(default_factory=dict, hash=False)
    #: PARTIAL by default, and that direction is deliberate. COMPLETE is the value
    #: that licenses Phase 2.4 to mark every posting this instance owns and did not
    #: deliver as absent, so a target that forgot to declare a scope would fail OPEN:
    #: one omitted keyword argument, and a search adapter retires a board's whole
    #: inventory. Defaulting to PARTIAL makes the omission cost a marking that does
    #: not happen (postings linger, visibly stale) instead of one that should never
    #: have happened. Every adapter states its scope explicitly regardless — see the
    #: registry-wide pin in `test_source_contract.py`.
    inventory_scope: InventoryScope = InventoryScope.PARTIAL
    host: str | None = None
    deadline_seconds: float | None = None

    def __post_init__(self) -> None:
        set_ = object.__setattr__
        set_(self, "source_key", (self.source_key or "").strip())
        set_(self, "instance_key", (self.instance_key or "").strip())
        set_(self, "label", (self.label or "").strip() or self.instance_key or self.source_key)
        set_(self, "params", _freeze(self.params))
        if not self.source_key:
            raise ConfigError("SourceTarget requires a source_key")
        if self.deadline_seconds is not None and self.deadline_seconds <= 0:
            raise ConfigError(
                "SourceTarget.deadline_seconds must be positive", source_key=self.source_key
            )

    @property
    def source_run_key(self) -> str:
        return f"{self.source_key}:{self.instance_key}" if self.instance_key else self.source_key

    @property
    def namespace(self) -> str:
        """Identity namespace records from this target must carry."""
        return self.source_run_key

    def param(self, key: str, default: JSONValue = None) -> JSONValue:
        return self.params.get(key, default)

    def require(self, key: str) -> JSONValue:
        value = self.params.get(key)
        if value in (None, ""):
            raise ConfigError(
                f"{self.source_run_key}: missing required param {key!r}",
                source_key=self.source_key,
                instance_key=self.instance_key,
            )
        return value

    def config_fingerprint(self) -> str:
        """Digest of everything that would invalidate a stored checkpoint.

        A checkpoint records "I got this far through *this* query". If the
        search terms, the tenant, or the page size changed, the cursor points
        into a different result set and resuming would skip real postings, so
        the scheduler discards it and starts clean.
        """
        return _stable_digest(
            {
                "source_key": self.source_key,
                "instance_key": self.instance_key,
                "params": dict(self.params),
                "inventory_scope": str(self.inventory_scope),
            }
        )

    def record(self, **kwargs: Any) -> NormalizedPosting:
        """Build a record already stamped with this target's identity.

        Adapters use this rather than constructing `NormalizedPosting`
        directly, so `source_key`/`instance_key` can never drift from the
        target that is being enumerated (which would corrupt the namespace and
        therefore the identity).
        """
        kwargs.setdefault("source_key", self.source_key)
        kwargs.setdefault("instance_key", self.instance_key)
        return NormalizedPosting(**kwargs)


# --------------------------------------------------------------------------- #
# Checkpoints
# --------------------------------------------------------------------------- #
CHECKPOINT_VERSION = 1


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """Resumable position inside one target's enumeration.

    Semantics, all of which the scheduler relies on:

      * OPAQUE. `cursor` is adapter-private. The scheduler stores it in
        `source_runs.checkpoint_json` and hands it back; it never inspects it.
      * ADVISORY. Ignoring a checkpoint is always safe, merely slower. An
        adapter with `supports_checkpoint=False` receives `None` and must not
        care.
      * DELIVERED, NOT COMMITTED. `ctx.mark_checkpoint()` is called after the
        records preceding it have been *yielded to the consumer*, because an
        async generator only advances when pulled. Whether the consumer had
        committed them when the process died is unknowable from here, so:
      * REPLAYABLE. Resuming may re-emit records that were already written.
        This is expected and safe: the writer dedupes on identity (invariant 5).
        Adapters must never treat a checkpoint as an at-most-once guarantee.
      * SCOPED. Valid only for the same target and the same
        `config_fingerprint`. `is_valid_for()` is the check; the scheduler
        discards mismatches rather than resuming into a different result set.
    """

    source_key: str
    instance_key: str
    cursor: Mapping[str, JSONValue]
    config_fingerprint: str
    emitted: int = 0
    version: int = CHECKPOINT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "cursor", _freeze(self.cursor))
        if self.emitted < 0:
            raise ValueError("Checkpoint.emitted must be >= 0")

    def to_json(self) -> str:
        return json.dumps(self.to_json_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    def to_json_dict(self) -> dict[str, JSONValue]:
        return {
            "version": self.version,
            "source_key": self.source_key,
            "instance_key": self.instance_key,
            "cursor": dict(self.cursor),
            "config_fingerprint": self.config_fingerprint,
            "emitted": self.emitted,
        }

    @classmethod
    def from_json_dict(cls, payload: Mapping[str, JSONValue]) -> Checkpoint:
        version = int(payload.get("version") or 0)
        if version != CHECKPOINT_VERSION:
            raise ValueError(f"unsupported checkpoint version {version!r}")
        return cls(
            source_key=payload.get("source_key") or "",
            instance_key=payload.get("instance_key") or "",
            cursor=payload.get("cursor") or {},
            config_fingerprint=payload.get("config_fingerprint") or "",
            emitted=int(payload.get("emitted") or 0),
            version=version,
        )

    @classmethod
    def from_json(cls, blob: str | bytes | None) -> Checkpoint | None:
        """Parse `source_runs.checkpoint_json`. `None`/empty means "no resume".

        A malformed or version-mismatched blob raises; the scheduler treats
        that as "start clean", which is always correct because checkpoints are
        advisory.
        """
        if blob is None:
            return None
        text = blob.decode("utf-8") if isinstance(blob, bytes) else blob
        if not text.strip():
            return None
        return cls.from_json_dict(json.loads(text))

    def is_valid_for(self, target: SourceTarget) -> bool:
        return (
            self.version == CHECKPOINT_VERSION
            and self.source_key == target.source_key
            and self.instance_key == target.instance_key
            and self.config_fingerprint == target.config_fingerprint()
        )


# --------------------------------------------------------------------------- #
# Transport seam
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class HttpRequest:
    """A request an adapter wants made. Data only — it performs nothing.

    Note what is absent: no timeout, no retry count, no backoff. Those belong
    to the scheduler-owned transport so that one policy governs every source.
    """

    url: str
    method: str = "GET"
    params: Mapping[str, JSONValue] | None = None
    headers: Mapping[str, str] | None = None
    json_body: JSONValue = None

    @property
    def host(self) -> str:
        """Per-host concurrency key for the scheduler's limiter."""
        return (urlsplit(self.url).hostname or "").lower()


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """A response as the adapter sees it. Bytes in, parsing is the adapter's job."""

    status: int
    url: str
    content: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "headers", _freeze(self.headers))

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self, *, source_key: str = "", instance_key: str = "") -> JSONValue:
        """Decode as JSON, raising `PayloadError` (permanent) on garbage.

        A 200 carrying HTML instead of JSON means the source changed or is
        serving an interstitial; retrying is pointless and the adapter is what
        needs fixing, so this is deliberately not transient.
        """
        try:
            return json.loads(self.content)
        except (ValueError, UnicodeDecodeError) as exc:
            raise PayloadError(
                f"response body is not JSON: {exc}",
                source_key=source_key,
                instance_key=instance_key,
                status=self.status,
                url=self.url,
            ) from exc


@runtime_checkable
class Transport(Protocol):
    """The only I/O primitive an adapter may touch.

    Implementations are owned and constructed by the scheduler (Phase 2.3),
    which is where connection pooling, per-host limits, politeness pacing, the
    socket timeout, and TLS policy live. `send` either returns a response — of
    any status, including 4xx/5xx, which the adapter classifies with
    `check_status` — or raises a `SourceError` for a transport-level failure.

    It must NOT retry. The single permitted retry is a scheduler decision made
    with knowledge of the run's remaining deadline budget.
    """

    async def send(self, request: HttpRequest) -> HttpResponse: ...


def check_status(
    response: HttpResponse,
    *,
    source_key: str = "",
    instance_key: str = "",
    allow: Iterable[int] = (200,),
) -> HttpResponse:
    """Assert an acceptable status, or raise the correctly classified error.

    This is the replacement for `scraper.py`'s `if r.status_code != 200: return
    out`. Returning an empty list there made a blocked, throttled, or
    404-ing source look identical to an empty one — the exact failure mode that
    Phase 2.4 absence marking cannot tolerate (invariant 3).
    """
    if response.status in tuple(allow):
        return response
    disposition = classify_status(response.status)
    cls = TransientSourceError if disposition is Disposition.TRANSIENT else PermanentSourceError
    raise cls(
        f"unexpected HTTP {response.status}",
        source_key=source_key,
        instance_key=instance_key,
        status=response.status,
        url=response.url,
    )


# --------------------------------------------------------------------------- #
# Fetch context
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class InboundPayload:
    """A payload handed to an adapter instead of being fetched.

    Two uses, and they are the same mechanism:

      * MANUAL IMPORT. The MCP arm (Dice, ZipRecruiter) has no transport at
        all; rows are produced out-of-band. Rather than bolt a second,
        weaker contract onto the scheduler for it, the importer supplies the
        rows as an `InboundPayload` and a `PUSH` adapter parses them with the
        same pure parser every other source uses. Manual import is therefore
        not a special case — it is a scraper whose transport already ran.
      * REPLAY. A frozen fixture is an `InboundPayload`, which is why fixture
        tests and manual import exercise identical code.
    """

    locator: str
    content: bytes
    media_type: str = "application/json"
    metadata: Mapping[str, JSONValue] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _freeze(self.metadata))

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> JSONValue:
        try:
            return json.loads(self.content)
        except (ValueError, UnicodeDecodeError) as exc:
            raise PayloadError(f"inbound payload {self.locator} is not JSON: {exc}") from exc


class FetchContext:
    """Everything one `fetch` call is allowed to reach, and its progress channel.

    Mutable by design (the checkpoint slot is written during the stream) and
    single-use: the scheduler builds a fresh context per attempt, so a retry
    can never inherit a half-advanced cursor.
    """

    __slots__ = ("_checkpoint", "_transport", "config", "deadline_at", "payloads", "resume_from")

    def __init__(
        self,
        *,
        config: SourceConfig | None = None,
        transport: Transport | None = None,
        resume_from: Checkpoint | None = None,
        payloads: Sequence[InboundPayload] = (),
        deadline_at: float | None = None,
    ) -> None:
        self.config = config if config is not None else SourceConfig()
        self._transport = transport
        #: Checkpoint to resume from, already validated by the scheduler against
        #: the target. `None` means start clean.
        self.resume_from = resume_from
        self.payloads = tuple(payloads)
        #: `time.monotonic()` value at which the scheduler will cancel this
        #: attempt. Advisory: for logging and for an optional cooperative stop
        #: at a page boundary. Enforcement is the scheduler's `asyncio` timeout,
        #: never an adapter's `if` (invariant 8).
        self.deadline_at = deadline_at
        self._checkpoint: Checkpoint | None = None

    def http(self) -> Transport:
        """The transport, or `ConfigError` if the scheduler withheld one.

        Raising rather than lazily constructing a client is deliberate: an
        adapter that reaches the network outside the scheduler's pool escapes
        its concurrency and politeness limits.
        """
        if self._transport is None:
            raise ConfigError("this fetch context has no transport (declared TransportKind.NONE?)")
        return self._transport

    @property
    def has_transport(self) -> bool:
        return self._transport is not None

    def mark_checkpoint(self, cursor: Mapping[str, JSONValue], *, target: SourceTarget, emitted: int = 0) -> Checkpoint:
        """Record "everything before this point has been yielded".

        Call it at page boundaries, after the page's records have been yielded.
        Because an async generator only resumes when pulled, reaching this line
        proves the consumer took delivery of every prior record.
        """
        checkpoint = Checkpoint(
            source_key=target.source_key,
            instance_key=target.instance_key,
            cursor=cursor,
            config_fingerprint=target.config_fingerprint(),
            emitted=emitted,
        )
        self._checkpoint = checkpoint
        return checkpoint

    @property
    def checkpoint(self) -> Checkpoint | None:
        """Latest checkpoint marked during this attempt, if any.

        The scheduler reads this after the stream ends — including when it ends
        by raising — so a partial success still persists its progress.
        """
        return self._checkpoint

    def remaining_seconds(self) -> float | None:
        """Advisory time left, or `None` when no deadline was supplied."""
        if self.deadline_at is None:
            return None
        return self.deadline_at - time.monotonic()


# --------------------------------------------------------------------------- #
# Descriptor and adapter protocol
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SourceDescriptor:
    """Static declaration of how the scheduler must treat one source.

    Everything the scheduler needs to plan a run without importing the
    adapter's body: which run kinds include it, how often it is due, how long
    it gets, whether it can resume, how it executes, and how hard it may lean
    on its host.
    """

    source_key: str
    category: SourceCategory
    run_kinds: frozenset[RunKind]
    #: Minimum age before a `daily` run considers this source due again.
    refresh_interval_seconds: int = 6 * 3600
    #: Wall-clock budget for ONE target attempt. The scheduler enforces it; the
    #: Success Contract's "one failed source adds no more than its own deadline"
    #: is this number.
    default_deadline_seconds: float = 30.0
    supports_checkpoint: bool = False
    execution: ExecutionMode = ExecutionMode.ASYNC_INPROCESS
    transport: TransportKind = TransportKind.HTTP
    #: Cap on this source's targets in flight at once (`None` = global limit
    #: only). Boards that share one API host need this even though each target
    #: is independent.
    max_concurrent_targets: int | None = None
    #: Hint for the scheduler's per-host limiter, keyed on `HttpRequest.host`.
    per_host_concurrency: int = 4
    #: Politeness floor between consecutive requests to the same host, applied
    #: by the transport. Replaces the `time.sleep(0.2)` calls in `scraper.py`,
    #: which adapters are forbidden from making themselves (invariant 2).
    min_request_interval_seconds: float = 0.0
    #: Whether records arrive with a usable description already attached, so
    #: Phase 3.2 can skip the description fetch.
    description_inline: bool = False
    #: Default for targets that do not override it. See `InventoryScope`, and
    #: `SourceTarget.inventory_scope` for why the default is the scope that licenses
    #: nothing: a descriptor that omits this must not thereby licence mass absence
    #: marking for every target it plans.
    default_inventory_scope: InventoryScope = InventoryScope.PARTIAL

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_key", (self.source_key or "").strip())
        object.__setattr__(self, "run_kinds", frozenset(self.run_kinds))
        if not self.source_key:
            raise ConfigError("SourceDescriptor requires a source_key")
        if not self.run_kinds:
            raise ConfigError(
                f"{self.source_key}: descriptor must declare at least one run kind",
                source_key=self.source_key,
            )
        if self.default_deadline_seconds <= 0:
            raise ConfigError(
                f"{self.source_key}: default_deadline_seconds must be positive",
                source_key=self.source_key,
            )
        if self.refresh_interval_seconds < 0:
            raise ConfigError(
                f"{self.source_key}: refresh_interval_seconds must be >= 0",
                source_key=self.source_key,
            )
        if self.per_host_concurrency < 1:
            raise ConfigError(
                f"{self.source_key}: per_host_concurrency must be >= 1",
                source_key=self.source_key,
            )
        if self.max_concurrent_targets is not None and self.max_concurrent_targets < 1:
            raise ConfigError(
                f"{self.source_key}: max_concurrent_targets must be >= 1 or None",
                source_key=self.source_key,
            )
        if (
            self.category in (SourceCategory.AGGREGATOR, SourceCategory.MANUAL)
            and self.default_inventory_scope is InventoryScope.COMPLETE
        ):
            # An aggregator answers a query and a manual import is whatever someone
            # pushed; neither can ever mean "these are all of them", so COMPLETE here
            # is not a tuning choice that happens to be wrong — it is a category
            # error, and the only thing it can produce is the mass retirement of
            # postings the source never claimed to enumerate.
            raise ConfigError(
                f"{self.source_key}: a {self.category} source may not declare "
                "default_inventory_scope=COMPLETE; it cannot enumerate an inventory",
                source_key=self.source_key,
            )

    def runs_in(self, kind: RunKind) -> bool:
        return kind in self.run_kinds

    def deadline_for(self, target: SourceTarget) -> float:
        return target.deadline_seconds or self.default_deadline_seconds


@runtime_checkable
class SourceAdapter(Protocol):
    """What all sixteen adapters implement.

    Two methods, split so the scheduler can plan a whole run without doing any
    I/O and without the adapter deciding anything about scheduling:

      `plan`   PURE and synchronous. Config in, targets out. No network, no
               clock, no database. Called once per run to build the work list.
               An unconfigured source returns `()`.
      `fetch`  An async generator over `NormalizedPosting`. One call handles
               exactly one target, and everything in this module's header
               applies to its body.

    Adapters are stateless singletons: the same instance serves every target
    concurrently, so instance attributes must be immutable configuration only.
    Per-attempt state belongs in local variables or on the `FetchContext`.
    """

    @property
    def descriptor(self) -> SourceDescriptor: ...

    def plan(self, config: SourceConfig) -> Sequence[SourceTarget]: ...

    def fetch(self, target: SourceTarget, ctx: FetchContext) -> AsyncIterator[NormalizedPosting]: ...
