"""Protocol adapters, one module per source shape.

Registration is explicit and eager: importing this package registers every
adapter that exists. A silently missing adapter would mean a source that never
runs, and Phase 2.4 would then mark its postings absent forever — so the
registry refuses duplicates and `install()` is the single wiring point.
"""
from __future__ import annotations

from .. import registry
from .amazon import ADAPTER as AMAZON_ADAPTER
from .ashby import ADAPTER as ASHBY_ADAPTER
from .builtin import ADAPTER as BUILTIN_ADAPTER
from .eightfold import ADAPTER as EIGHTFOLD_ADAPTER
from .greenhouse import ADAPTER as GREENHOUSE_ADAPTER
from .icims import ADAPTER as ICIMS_ADAPTER
from .jibe import ADAPTER as JIBE_ADAPTER
from .jobspy import ADAPTER as JOBSPY_ADAPTER
from .lever import ADAPTER as LEVER_ADAPTER
from .manual import ADAPTER as MANUAL_ADAPTER
from .phenom import ADAPTER as PHENOM_ADAPTER
from .recruitee import ADAPTER as RECRUITEE_ADAPTER
from .smartrecruiters import ADAPTER as SMARTRECRUITERS_ADAPTER
from .workable import ADAPTER as WORKABLE_ADAPTER
from .workday import ADAPTER as WORKDAY_ADAPTER
from .yc import ADAPTER as YC_ADAPTER

__all__ = ["ALL_ADAPTERS", "install"]

ALL_ADAPTERS = (
    AMAZON_ADAPTER,
    ASHBY_ADAPTER,
    BUILTIN_ADAPTER,
    EIGHTFOLD_ADAPTER,
    GREENHOUSE_ADAPTER,
    ICIMS_ADAPTER,
    JIBE_ADAPTER,
    JOBSPY_ADAPTER,
    LEVER_ADAPTER,
    MANUAL_ADAPTER,
    PHENOM_ADAPTER,
    RECRUITEE_ADAPTER,
    SMARTRECRUITERS_ADAPTER,
    WORKABLE_ADAPTER,
    WORKDAY_ADAPTER,
    YC_ADAPTER,
)


def install(*, replace: bool = False) -> None:
    """Register every adapter in this package.

    Idempotent with `replace=True`; called once at scheduler start-up. Kept as
    a function rather than an import side effect so tests can build an isolated
    registry without inheriting production registrations.
    """
    for adapter in ALL_ADAPTERS:
        if replace or not registry.is_registered(adapter.descriptor.source_key):
            registry.register(adapter, replace=True)


install()
