# Copyright 2026 the airflow-pytest-operator contributors
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

"""Airflow-Variable backed store for the failed node-ids of a task instance.

The ``Variable`` class is resolved through the compat shim
(:func:`~airflow_pytest_operator.compat.import_variable`), so this module
imports no Airflow directly. It degrades gracefully: if Airflow (or the
Variable backend) is unavailable, reads return ``[]`` and writes/deletes are
no-ops, so ``failed_only`` falls back to running the full suite -- it never
crashes the task over a bookkeeping store.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

_log = logging.getLogger(__name__)

_VAR_KEY_PREFIX = "apo_last_failed__"


def last_failed_var_key(context: Any) -> str | None:
    """A stable Airflow Variable key for this task instance's failed set.

    Derived from the Airflow ids ``(dag_id, task_id, run_id)`` -- and
    crucially **not** ``try_number`` -- so the key is identical across this
    task's retries (a retry reads what the previous attempt wrote) yet unique
    per task instance (parallel tasks never clobber each other's store). The
    ids are folded into a short hash so the key stays within Airflow's
    250-char Variable-key limit and never collides between different runs.
    Returns ``None`` when the ids are unavailable, so the operator simply runs
    the full suite rather than guessing a key.

    This function imports no Airflow -- it only reads attributes off
    ``context["ti"]`` -- so it is safe to call (and unit-test) anywhere.
    """
    ti = context.get("ti") if hasattr(context, "get") else None
    dag_id = getattr(ti, "dag_id", None)
    task_id = getattr(ti, "task_id", None)
    run_id = getattr(ti, "run_id", None)
    if not (
        isinstance(dag_id, str)
        and isinstance(task_id, str)
        and isinstance(run_id, str)
    ):
        return None
    # Not cryptographic -- just a stable, collision-resistant key suffix.
    # blake2b avoids the Bandit/CodeQL "weak hash" flag that sha1/md5 trigger.
    digest = hashlib.blake2b(
        f"{dag_id}|{task_id}|{run_id}".encode(), digest_size=8
    ).hexdigest()
    safe_task = re.sub(r"[^0-9A-Za-z._-]+", "_", task_id)[:80]
    return f"{_VAR_KEY_PREFIX}{safe_task}__{digest}"


def _ti_int_attr(context: Any, name: str) -> int | None:
    """Read an int attribute off ``context["ti"]``, or ``None`` if absent.

    ``isinstance(..., bool)`` is excluded because ``bool`` is an ``int``
    subclass and a stray ``True``/``False`` would otherwise pass as a count.
    Imports no Airflow -- it only reads attributes off the context.
    """
    ti = context.get("ti") if hasattr(context, "get") else None
    value = getattr(ti, name, None)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def is_final_attempt(context: Any) -> bool:
    """True when Airflow will NOT retry the task after this attempt.

    The final attempt is the one whose ``try_number`` exceeds ``max_tries``
    (e.g. ``retries=2`` -> ``max_tries=2`` with attempts 1, 2, 3; the third is
    final). Read defensively: if either value is unavailable we return
    ``False`` ("more retries may come"), erring toward keeping the store rather
    than discarding it too early. Lives here next to the store it governs: it
    decides when the failed_only Variable may be deleted (no further retry will
    read it). Imports no Airflow -- it only reads ``context["ti"]``.
    """
    try_number = _ti_int_attr(context, "try_number")
    max_tries = _ti_int_attr(context, "max_tries")
    if try_number is None or max_tries is None:
        return False
    return try_number > max_tries


class VariableLastFailedStore:
    """Read/write/delete a task instance's failed node-ids via an Airflow Variable.

    Pass ``variable_cls`` to inject a backend directly -- convenient for tests,
    which can hand in a fake instead of monkeypatching. With no injection, the
    Airflow ``Variable`` class is resolved through
    :func:`~airflow_pytest_operator.compat.import_variable`, which is itself
    ``lru_cache``-d, so the version-specific import happens once per process.

    Every method is best-effort and never raises: failures degrade to "no
    store" (an empty read, a skipped write/delete) so a bookkeeping problem
    can never mask or replace the real test outcome.
    """

    def __init__(self, variable_cls: type[Any] | None = None) -> None:
        # ``None`` means "resolve the Airflow Variable class on use via the
        # process-wide cached compat.import_variable"; a non-None value is an
        # injected backend used as-is.
        self._variable_cls = variable_cls

    def _cls(self) -> type[Any] | None:
        if self._variable_cls is not None:
            return self._variable_cls
        # Imported lazily (not at module load) so that importing the package
        # stays Airflow-free: the compat shim resolves BaseOperator at import
        # time, which we must not trigger until a task actually runs. The
        # resolver is lru_cache-d, so there's no second instance-level cache.
        from ..compat import import_variable

        return import_variable()

    def read(self, key: str) -> list[str]:
        """Return the stored list of failed node-ids, or ``[]`` if unavailable.

        A missing Variable, a missing backend, or a value that is not a list of
        strings all yield ``[]`` so the caller falls back to the full suite.
        """
        cls = self._cls()
        if cls is None:
            return []
        try:
            value = cls.get(key, deserialize_json=True)
        except Exception:
            # Missing key (KeyError on 2.x) or any backend error -> no store.
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return []

    def write(self, key: str, node_ids: list[str]) -> None:
        """Persist ``node_ids`` under ``key`` as a JSON list. Never raises."""
        cls = self._cls()
        if cls is None:
            return
        try:
            cls.set(key, list(node_ids), serialize_json=True)
        except Exception:  # pragma: no cover - best-effort bookkeeping
            _log.warning(
                "Could not write failed_only Variable %r", key, exc_info=True
            )

    def delete(self, key: str) -> None:
        """Delete the Variable ``key`` if present. Never raises."""
        cls = self._cls()
        if cls is None:
            return
        try:
            cls.delete(key)
        except Exception:  # pragma: no cover - best-effort bookkeeping
            # Already gone, or backend error: nothing more we can do.
            _log.debug(
                "Could not delete failed_only Variable %r", key, exc_info=True
            )
