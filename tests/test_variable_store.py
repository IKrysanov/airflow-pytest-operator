"""Tests for the Airflow-Variable backed failed_only store.

The store is the single Airflow touch point for the ``failed_only`` retry
strategy. These tests pin two things: the pure key derivation (no Airflow
needed) and that every backend method degrades gracefully -- a missing or
broken backend must never raise, so a bookkeeping problem can't mask the
real test outcome.
"""

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

from __future__ import annotations

from airflow_pytest_operator.stores import (
    VariableLastFailedStore,
    is_final_attempt,
    last_failed_var_key,
)


class _TI:
    def __init__(self, **attrs):
        for key, value in attrs.items():
            setattr(self, key, value)


def _ctx(**kwargs):
    return {"ti": _TI(**kwargs)}


# ---------------------------------------------------------------------------
# last_failed_var_key -- pure derivation, no Airflow required
# ---------------------------------------------------------------------------


def test_key_is_stable_and_prefixed():
    k1 = last_failed_var_key(_ctx(dag_id="d", task_id="my_task", run_id="r"))
    k2 = last_failed_var_key(_ctx(dag_id="d", task_id="my_task", run_id="r"))
    print(f"[key:stable] {k1}")
    assert k1 == k2                              # deterministic across calls
    assert k1.startswith("apo_last_failed__")
    assert "my_task" in k1                       # human-readable task segment


def test_key_differs_per_run_and_task():
    base = last_failed_var_key(_ctx(dag_id="d", task_id="t", run_id="r1"))
    other_run = last_failed_var_key(_ctx(dag_id="d", task_id="t", run_id="r2"))
    other_task = last_failed_var_key(_ctx(dag_id="d", task_id="t2", run_id="r1"))
    assert base != other_run
    assert base != other_task


def test_key_is_bounded_for_long_ids():
    long_run = "scheduled__" + "x" * 500
    k = last_failed_var_key(_ctx(dag_id="d" * 300, task_id="t" * 300, run_id=long_run))
    print(f"[key:bounded] len={len(k)}")
    assert k is not None
    assert len(k) <= 250                         # Airflow Variable-key limit


def test_key_sanitizes_task_id():
    k = last_failed_var_key(_ctx(dag_id="d", task_id="weird/task id!", run_id="r"))
    print(f"[key:sanitize] {k}")
    # Only the safe charset survives in the readable segment.
    assert "/" not in k
    assert " " not in k
    assert "!" not in k


def test_key_none_when_ids_missing():
    assert last_failed_var_key(_ctx()) is None              # all None
    assert last_failed_var_key(_ctx(dag_id="d", task_id="t")) is None  # no run_id
    assert last_failed_var_key({}) is None                  # no ti at all


# ---------------------------------------------------------------------------
# VariableLastFailedStore -- graceful degradation without a backend
# ---------------------------------------------------------------------------


def test_store_degrades_to_noop_without_backend(monkeypatch):
    # No Airflow Variable backend available (compat.import_variable returns
    # None) -> read is empty, write/delete are silent no-ops (never raise).
    monkeypatch.setattr("airflow_pytest_operator.compat.import_variable", lambda: None)
    store = VariableLastFailedStore()
    assert store.read("k") == []
    store.write("k", ["a::b"])   # must not raise
    store.delete("k")            # must not raise


# ---------------------------------------------------------------------------
# VariableLastFailedStore -- round-trip against an injected fake backend
# ---------------------------------------------------------------------------


def _make_fake():
    """A fresh in-memory stand-in mirroring airflow.Variable's classmethod API.

    A new class per call keeps tests isolated (no shared class-level state).
    """

    class _FakeVariable:
        backing: dict = {}
        set_calls: list = []

        @classmethod
        def get(cls, key, deserialize_json=False):
            if key not in cls.backing:
                raise KeyError(key)          # Airflow 2.x raises on missing
            return cls.backing[key]

        @classmethod
        def set(cls, key, value, serialize_json=False):
            cls.set_calls.append((key, value, serialize_json))
            cls.backing[key] = value

        @classmethod
        def delete(cls, key):
            del cls.backing[key]             # raises KeyError if missing

    return _FakeVariable


def test_store_round_trip():
    fake = _make_fake()
    store = VariableLastFailedStore(variable_cls=fake)  # injected backend

    store.write("k", ["tests.test_x::test_a", "tests.test_y::test_b"])
    # JSON serialization is requested so Airflow stores a real list, not a str.
    assert fake.set_calls[0][2] is True
    assert store.read("k") == ["tests.test_x::test_a", "tests.test_y::test_b"]

    store.delete("k")
    assert store.read("k") == []                 # gone -> empty


def test_store_resolves_class_once_and_caches():
    fake = _make_fake()
    store = VariableLastFailedStore(variable_cls=fake)
    # The injected class is cached on the instance and reused across calls.
    assert store._cls() is fake
    store.write("k", ["a::b"])
    assert store._variable_cls is fake


def test_store_read_missing_returns_empty():
    assert VariableLastFailedStore(variable_cls=_make_fake()).read("absent") == []


def test_store_read_non_list_returns_empty():
    fake = _make_fake()
    fake.backing["k"] = {"not": "a list"}
    assert VariableLastFailedStore(variable_cls=fake).read("k") == []


def test_store_read_coerces_items_to_str():
    fake = _make_fake()
    fake.backing["k"] = ["a::b", 123]
    assert VariableLastFailedStore(variable_cls=fake).read("k") == ["a::b", "123"]


def test_store_delete_missing_does_not_raise():
    VariableLastFailedStore(variable_cls=_make_fake()).delete("never-written")


# ---------------------------------------------------------------------------
# is_final_attempt -- decides when the failed_only Variable may be deleted
# ---------------------------------------------------------------------------


def test_is_final_attempt_true_on_last_attempt():
    # retries=2 -> max_tries=2; attempts try_number 1, 2, 3; the 3rd is final.
    assert is_final_attempt(_ctx(try_number=3, max_tries=2)) is True


def test_is_final_attempt_false_mid_cycle():
    assert is_final_attempt(_ctx(try_number=1, max_tries=2)) is False
    assert is_final_attempt(_ctx(try_number=2, max_tries=2)) is False


def test_is_final_attempt_false_when_values_missing():
    assert is_final_attempt(_ctx(try_number=9)) is False   # no max_tries
    assert is_final_attempt(_ctx(max_tries=2)) is False     # no try_number
    assert is_final_attempt({}) is False
    assert is_final_attempt(None) is False


def test_is_final_attempt_ignores_bool_attrs():
    # bool is an int subclass; a stray True/False must not be read as a count.
    assert is_final_attempt(_ctx(try_number=True, max_tries=2)) is False
    assert is_final_attempt(_ctx(try_number=3, max_tries=False)) is False
