"""Typed source-adapter contract, registry, and protocol adapters (Phase 2).

`contract` is the authoritative module: every type and every invariant the
sixteen adapters and the concurrent scheduler are built against lives there and
is documented there. This package re-exports the contract surface so callers
write `from backend.sources import NormalizedPosting` rather than reaching into
submodules.

Import layering, which is deliberate and worth preserving:
  contract   pure stdlib. No third-party imports, no I/O, no database.
  registry   depends on contract only.
  adapters   depend on contract only; `adapters/__init__` also on registry.
  transport  the one module that touches httpx, imported lazily so the rest of
             the package (and every parser test) works without it.
  testing    fakes and frozen-fixture loading, shared by all adapter tests.
"""
from __future__ import annotations

from .contract import (
    CANONICAL_HASH_FIELDS,
    CHECKPOINT_VERSION,
    Checkpoint,
    ConfigError,
    Disposition,
    ExecutionMode,
    FetchContext,
    HttpRequest,
    HttpResponse,
    IdentityClaim,
    InboundPayload,
    InventoryScope,
    NormalizedPosting,
    PayloadError,
    PermanentSourceError,
    RunKind,
    SourceAdapter,
    SourceCategory,
    SourceConfig,
    SourceDescriptor,
    SourceError,
    SourceTarget,
    TransientSourceError,
    Transport,
    TransportKind,
    check_status,
    classify_status,
    normalize_date,
    normalize_text,
    normalize_url,
)

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
    "normalize_date",
    "normalize_text",
    "normalize_url",
    "registry",
]

from . import registry  # noqa: E402  (re-exported after the contract surface)
