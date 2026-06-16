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

"""Integration tests against a REAL Airflow ``Variable`` backend.

The unit suite stubs ``BaseOperator`` and has no Variable backend, so
``VariableLastFailedStore`` degrades to a no-op there and the round-trip can
only be tested against an in-memory fake. These tests pin what the fake
cannot: that the *real* Airflow ``Variable`` API matches what the store calls
-- the ``get``/``set``/``delete`` surface and the ``deserialize_json`` /
``serialize_json`` keyword arguments -- on whichever Airflow (2.x or 3.x) is
installed. They run only in the integration CI job (real Airflow present); the
whole module skips when no Variable backend resolves.
"""

from __future__ import annotations

import inspect

import pytest

from airflow_pytest_operator.compat import import_variable
from airflow_pytest_operator.stores import VariableLastFailedStore, last_failed_var_key

# Resolve the real Variable class once. In the unit job (stubbed Airflow, no
# Variable) this is None and the whole module is skipped; in the integration
# job it is the genuine airflow.sdk / airflow.models Variable.
_VARIABLE_CLS = import_variable()

pytestmark = pytest.mark.skipif(
    _VARIABLE_CLS is None,
    reason="no real Airflow Variable backend (unit/stub environment)",
)

_TEST_KEY = "apo_integration_roundtrip__pytest"


class _FakeTI:
    dag_id = "apo_integration_dag"
    task_id = "apo_integration_task"
    run_id = "manual__2026-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# API-surface contract: no metadata DB required, so these always run when a
# real Variable class resolved. They catch a renamed/removed method or kwarg
# (which would silently degrade failed_only to a full-suite run) at the exact
# point the store depends on it.
# ---------------------------------------------------------------------------


def test_real_variable_exposes_get_set_delete():
    for name in ("get", "set", "delete"):
        assert callable(getattr(_VARIABLE_CLS, name, None)), (
            f"Airflow Variable is missing a callable {name!r}; "
            "VariableLastFailedStore relies on it"
        )


def test_real_variable_get_accepts_deserialize_json_kwarg():
    params = inspect.signature(_VARIABLE_CLS.get).parameters
    assert "deserialize_json" in params, (
        "Airflow Variable.get no longer accepts deserialize_json; "
        "VariableLastFailedStore.read passes it"
    )


def test_real_variable_set_accepts_serialize_json_kwarg():
    params = inspect.signature(_VARIABLE_CLS.set).parameters
    assert "serialize_json" in params, (
        "Airflow Variable.set no longer accepts serialize_json; "
        "VariableLastFailedStore.write passes it"
    )


# ---------------------------------------------------------------------------
# Full round-trip through the real backend. Needs an initialised Airflow
# metadata DB. The store is best-effort (write/read swallow backend errors), so
# when no DB is configured a write degrades to a no-op and the read comes back
# empty -- we detect that and skip rather than fail. The CI integration job
# runs ``airflow db migrate`` first, so there the round-trip actually executes.
# ---------------------------------------------------------------------------


def test_store_round_trip_through_real_variable():
    store = VariableLastFailedStore()  # resolves the real Variable class
    node_ids = ["tests.test_x::test_a", "tests.test_y::test_b[1::2]"]

    store.delete(_TEST_KEY)  # clean slate (no-op if absent)
    try:
        store.write(_TEST_KEY, node_ids)
        got = store.read(_TEST_KEY)
        if not got:
            pytest.skip(
                "no usable Airflow metadata DB -- write degraded to a no-op; "
                "run 'airflow db migrate' to exercise the real round-trip"
            )
        # JSON serialization must preserve the list of strings exactly,
        # including the '::' inside a parametrized id.
        assert got == node_ids
        store.delete(_TEST_KEY)
        assert store.read(_TEST_KEY) == []  # gone -> empty
    finally:
        store.delete(_TEST_KEY)


def test_store_round_trip_with_derived_key():
    # Exercise the real key derivation alongside the real backend, the way the
    # operator wires them together.
    store = VariableLastFailedStore()
    key = last_failed_var_key({"ti": _FakeTI()})
    assert key is not None and key.startswith("apo_last_failed__")

    store.delete(key)
    try:
        store.write(key, ["tests.test_z::test_c"])
        got = store.read(key)
        if not got:
            pytest.skip("no usable Airflow metadata DB for the round-trip")
        assert got == ["tests.test_z::test_c"]
    finally:
        store.delete(key)
