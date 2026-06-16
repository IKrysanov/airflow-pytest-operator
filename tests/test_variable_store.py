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
    assert k1 == k2  # deterministic across calls
    assert k1.startswith("apo_last_failed__")
    assert "my_task" in k1  # human-readable task segment


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
    assert len(k) <= 250  # Airflow Variable-key limit


def test_key_sanitizes_task_id():
    k = last_failed_var_key(_ctx(dag_id="d", task_id="weird/task id!", run_id="r"))
    print(f"[key:sanitize] {k}")
    # Only the safe charset survives in the readable segment.
    assert "/" not in k
    assert " " not in k
    assert "!" not in k


def test_key_none_when_ids_missing():
    assert last_failed_var_key(_ctx()) is None  # all None
    assert last_failed_var_key(_ctx(dag_id="d", task_id="t")) is None  # no run_id
    assert last_failed_var_key({}) is None  # no ti at all


def test_key_differs_per_map_index():
    # Dynamically-mapped instances of one task share (dag_id, task_id, run_id)
    # and differ only by map_index -- they must get DISTINCT keys so they never
    # clobber each other's failed set.
    base = last_failed_var_key(_ctx(dag_id="d", task_id="t", run_id="r", map_index=0))
    other = last_failed_var_key(_ctx(dag_id="d", task_id="t", run_id="r", map_index=1))
    print(f"[key:map_index] {base} vs {other}")
    assert base != other


def test_key_stable_across_retries_for_same_map_index():
    # The key must NOT depend on try_number, so a retry of the same mapped
    # instance reads what the previous attempt of that same map_index wrote.
    k1 = last_failed_var_key(
        _ctx(dag_id="d", task_id="t", run_id="r", map_index=3, try_number=1)
    )
    k2 = last_failed_var_key(
        _ctx(dag_id="d", task_id="t", run_id="r", map_index=3, try_number=2)
    )
    assert k1 == k2


def test_key_unmapped_normalises_to_minus_one():
    # A non-mapped task (map_index == -1, Airflow's sentinel) must derive the
    # same key whether map_index is absent from the context or explicitly -1,
    # and that key must differ from any real mapped index.
    absent = last_failed_var_key(_ctx(dag_id="d", task_id="t", run_id="r"))
    explicit = last_failed_var_key(
        _ctx(dag_id="d", task_id="t", run_id="r", map_index=-1)
    )
    mapped0 = last_failed_var_key(
        _ctx(dag_id="d", task_id="t", run_id="r", map_index=0)
    )
    print(f"[key:unmapped] absent={absent} explicit={explicit} mapped0={mapped0}")
    assert absent == explicit
    assert mapped0 != absent


# ---------------------------------------------------------------------------
# VariableLastFailedStore -- graceful degradation without a backend
# ---------------------------------------------------------------------------


def test_store_degrades_to_noop_without_backend(monkeypatch):
    # No Airflow Variable backend available (compat.import_variable returns
    # None) -> read is empty, write/delete are silent no-ops (never raise).
    monkeypatch.setattr("airflow_pytest_operator.compat.import_variable", lambda: None)
    store = VariableLastFailedStore()
    assert store.read("k") == []
    store.write("k", ["a::b"])  # must not raise
    store.delete("k")  # must not raise


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
                raise KeyError(key)  # Airflow 2.x raises on missing
            return cls.backing[key]

        @classmethod
        def set(cls, key, value, serialize_json=False):
            cls.set_calls.append((key, value, serialize_json))
            cls.backing[key] = value

        @classmethod
        def delete(cls, key):
            del cls.backing[key]  # raises KeyError if missing

    return _FakeVariable


def test_store_round_trip():
    fake = _make_fake()
    store = VariableLastFailedStore(variable_cls=fake)  # injected backend

    store.write("k", ["tests.test_x::test_a", "tests.test_y::test_b"])
    # JSON serialization is requested so Airflow stores a real list, not a str.
    assert fake.set_calls[0][2] is True
    assert store.read("k") == ["tests.test_x::test_a", "tests.test_y::test_b"]

    store.delete("k")
    assert store.read("k") == []  # gone -> empty


def test_store_uses_injected_class_as_is():
    # An injected backend is used verbatim -- the store never falls back to the
    # auto-resolved compat.import_variable when ``variable_cls`` was given.
    fake = _make_fake()
    store = VariableLastFailedStore(variable_cls=fake)
    store.write("k", ["a::b"])
    assert store.read("k") == ["a::b"]  # round-tripped through the fake
    assert fake.set_calls  # the injected class was the one used


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
# is_final_attempt -- decides when the failed_only Variable may be written
# forward (public helper, exported from airflow_pytest_operator.stores).
# ---------------------------------------------------------------------------


def test_is_final_attempt_true_on_last_attempt():
    # retries=2 -> max_tries=2; attempts try_number 1, 2, 3; the 3rd is final.
    assert is_final_attempt(_ctx(try_number=3, max_tries=2)) is True


def test_is_final_attempt_false_mid_cycle():
    assert is_final_attempt(_ctx(try_number=1, max_tries=2)) is False
    assert is_final_attempt(_ctx(try_number=2, max_tries=2)) is False


def test_is_final_attempt_false_at_boundary_equal():
    # try_number == max_tries is NOT final (one more retry remains): the
    # comparison is strict ``>``. Pins it against an accidental ``>=``.
    assert is_final_attempt(_ctx(try_number=2, max_tries=2)) is False


def test_is_final_attempt_false_when_values_missing():
    assert is_final_attempt(_ctx(try_number=9)) is False  # no max_tries
    assert is_final_attempt(_ctx(max_tries=2)) is False  # no try_number
    assert is_final_attempt({}) is False
    assert is_final_attempt(None) is False


def test_is_final_attempt_ignores_bool_attrs():
    # bool is an int subclass; a stray True/False must not be read as a count.
    assert is_final_attempt(_ctx(try_number=True, max_tries=2)) is False
    assert is_final_attempt(_ctx(try_number=3, max_tries=False)) is False


class _SpyLog:
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args):
        # Mirror logging's %-formatting so the test sees the rendered text.
        self.warnings.append(msg % args if args else msg)


def test_is_final_attempt_warns_to_log_when_undeterminable():
    # When try_number/max_tries can't be read AND a log is supplied, the
    # "may orphan a Variable" risk is surfaced to the (task) log.
    log = _SpyLog()
    assert is_final_attempt(_ctx(try_number=9), log=log) is False  # no max_tries
    assert len(log.warnings) == 1
    assert "final attempt" in log.warnings[0]


def test_is_final_attempt_does_not_warn_when_determinable():
    # A clear answer (either direction) never warns, even with a log present.
    log = _SpyLog()
    assert is_final_attempt(_ctx(try_number=3, max_tries=2), log=log) is True
    assert is_final_attempt(_ctx(try_number=1, max_tries=2), log=log) is False
    assert log.warnings == []


def test_is_final_attempt_undeterminable_silent_without_log():
    # No log supplied -> still no raise, no warning (back-compatible default).
    assert is_final_attempt(_ctx(try_number=9)) is False
