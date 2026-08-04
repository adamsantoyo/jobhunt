"""Adapter registry, keyed by `source_key`.

One process-wide table so the scheduler can turn a run kind plus a config into
a work list without knowing any adapter by name. Registration is explicit
(`adapters/__init__.py` imports each module and calls `register`) rather than
import-scanning: a silently missing adapter would mean a source that never
runs, and Phase 2.4 would then mark its postings absent forever.
"""
from __future__ import annotations

from collections.abc import Sequence

from .contract import ConfigError, RunKind, SourceAdapter, SourceConfig, SourceTarget

__all__ = ["all_adapters", "get", "is_registered", "keys", "plan_run", "register", "unregister"]

_REGISTRY: dict[str, SourceAdapter] = {}


def register(adapter: SourceAdapter, *, replace: bool = False) -> SourceAdapter:
    """Add an adapter. Duplicate keys raise unless `replace=True`.

    Duplicates are an error rather than a last-wins overwrite because two
    adapters sharing a `source_key` would share an identity namespace, and the
    resolver would merge unrelated postings.
    """
    key = adapter.descriptor.source_key
    if not key:
        raise ConfigError("adapter descriptor has no source_key")
    if not replace and key in _REGISTRY:
        raise ConfigError(f"duplicate source_key {key!r} in adapter registry", source_key=key)
    _REGISTRY[key] = adapter
    return adapter


def unregister(source_key: str) -> None:
    """Remove an adapter. Exists for tests; production code never calls it."""
    _REGISTRY.pop(source_key, None)


def get(source_key: str) -> SourceAdapter:
    try:
        return _REGISTRY[source_key]
    except KeyError:
        raise ConfigError(f"no adapter registered for source_key {source_key!r}", source_key=source_key) from None


def is_registered(source_key: str) -> bool:
    return source_key in _REGISTRY


def keys() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


def all_adapters() -> tuple[SourceAdapter, ...]:
    return tuple(_REGISTRY[k] for k in sorted(_REGISTRY))


def plan_run(config: SourceConfig, run_kind: RunKind) -> list[tuple[SourceAdapter, SourceTarget]]:
    """Every (adapter, target) pair a run of `run_kind` should schedule.

    Pure: no I/O, no clock. "Which of these are *due*" is a separate scheduler
    decision made against `source_runs` history and
    `descriptor.refresh_interval_seconds`; this function answers only "which
    are eligible". Order is stable (by source key, then plan order) so a run
    plan is reproducible and diffable.
    """
    planned: list[tuple[SourceAdapter, SourceTarget]] = []
    for adapter in all_adapters():
        if not adapter.descriptor.runs_in(run_kind):
            continue
        for target in adapter.plan(config):
            if target.source_key != adapter.descriptor.source_key:
                raise ConfigError(
                    f"{adapter.descriptor.source_key}: plan() returned a target for "
                    f"{target.source_key!r}",
                    source_key=adapter.descriptor.source_key,
                )
            planned.append((adapter, target))
    return planned


def targets_for(source_key: str, config: SourceConfig) -> Sequence[SourceTarget]:
    return get(source_key).plan(config)
