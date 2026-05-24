"""Airflow version compatibility shim.

This is the *only* module in the package that imports Airflow directly.
Everything else imports ``BaseOperator`` from here. Centralizing the
version-specific imports means that supporting a new Airflow release is
a one-file change (Open/Closed at the package level).

Airflow 2.x and 3.x differ in the import path of ``BaseOperator`` and a
few helpers. We resolve them once, lazily, and expose a stable surface.
"""

# Copyright 2026 Ilya Krysanov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # Static analysers (mypy/Pylance) don't have Airflow installed and
    # can't follow the runtime try/except below. We give them a concrete
    # but minimal type to bind ``BaseOperator`` to, so downstream code is
    # type-checkable. At runtime this block is skipped entirely.
    class BaseOperator:  # noqa: D101 - stub for type-checking only
        task_id: str
        log: Any

        def __init__(self, *args: Any, **kwargs: Any) -> None: ...


@lru_cache(maxsize=1)
def get_airflow_version() -> tuple[int, ...]:
    try:
        from airflow.version import version as _v
    except Exception:  # pragma: no cover - airflow always ships this
        return (0,)
    parts: list[int] = []
    for chunk in _v.split(".")[:3]:
        num = "".join(ch for ch in chunk if ch.isdigit())
        parts.append(int(num) if num else 0)
    return tuple(parts)


def _import_base_operator() -> type[Any]:
    """Return the correct BaseOperator class for the installed Airflow.

    Resolution order is by import location, most-preferred first, so that
    the deprecated ``airflow.models.baseoperator`` path is only ever reached
    on Airflow 2 (where it's the sole location) and never on Airflow 3
    (where importing it emits a DeprecatedImportWarning):

      1. ``airflow.sdk.bases.operator``  -- canonical on all Airflow 3.x
      2. ``airflow.sdk``                 -- top-level re-export (early 3.0.x,
                                            and the test stub for Airflow 3)
      3. ``airflow.models.baseoperator`` -- Airflow 2.x only

    Each step is tried independently; we never fall through to step 3 once
    a 3.x SDK import has succeeded, which is what avoids the deprecation.
    """
    # 1. Canonical Task SDK location (Airflow 3.x, no deprecation).
    try:
        from airflow.sdk.bases.operator import (  # type: ignore[import-not-found]
            BaseOperator,
        )

        return BaseOperator  # type: ignore[no-any-return]
    except Exception:
        pass

    # 2. Top-level re-export: early 3.0.x releases and the test stub.
    try:
        from airflow.sdk import BaseOperator  # type: ignore[attr-defined]

        return BaseOperator  # type: ignore[no-any-return]
    except Exception:
        pass

    # 3. Airflow 2.x only.
    from airflow.models.baseoperator import BaseOperator

    return BaseOperator  # type: ignore[no-any-return]


def _import_apply_defaults() -> Any:
    """``apply_defaults`` is a no-op decorator in 2.x and gone in 3.x.

    We return a passthrough when it's unavailable so operator code can
    reference it uniformly without branching.
    """
    try:
        from airflow.utils.decorators import (  # type: ignore[attr-defined]
            apply_defaults,
        )

        return apply_defaults
    except Exception:

        def apply_defaults(func: Any) -> Any:
            return func

        return apply_defaults


# Resolved at import time, but cheap and side-effect-free.
# The TYPE_CHECKING stub above already bound ``BaseOperator`` for analysers;
# this is the real runtime value, hence the explicit no-redef suppression.
BaseOperator = _import_base_operator()  # type: ignore[assignment,no-redef,misc]
apply_defaults = _import_apply_defaults()

__all__ = ["BaseOperator", "apply_defaults", "get_airflow_version"]
