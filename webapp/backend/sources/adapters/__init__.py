"""Protocol adapters, one module per source shape.

Registration is explicit and eager: importing this package registers every
adapter that exists. Phase 2.2 adds the remaining fifteen modules here
(lever, ashby, smartrecruiters, workable, recruitee, workday, eightfold,
microsoft, amazon, icims, phenom, jibe, costco, builtin, yc, jobspy, manual).
"""
from __future__ import annotations

from .. import registry
from .greenhouse import ADAPTER as GREENHOUSE_ADAPTER

__all__ = ["GREENHOUSE_ADAPTER", "install"]


def install(*, replace: bool = False) -> None:
    """Register every adapter in this package.

    Idempotent with `replace=True`; called once at scheduler start-up. Kept as
    a function rather than an import side effect so tests can build an isolated
    registry without inheriting production registrations.
    """
    for adapter in (GREENHOUSE_ADAPTER,):
        if replace or not registry.is_registered(adapter.descriptor.source_key):
            registry.register(adapter, replace=True)


install()
