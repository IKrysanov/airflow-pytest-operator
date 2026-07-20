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


"""failed_only hardening: the stored set is untrusted input.

Whoever can write the Airflow Variable controls strings that become pytest
positional arguments. Since pytest parses a leading "-" as an option, an entry
like "-p evil_module" would load an arbitrary plugin -- code execution on the
worker. Option-like entries must never reach pytest. Shared fakes in
_op_helpers."""

from __future__ import annotations

import logging

import pytest
from _op_helpers import FakeParser, FakeRunner, FakeStore, _ctx, _key, _result

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def _op(stored, **kwargs):
    """Operator in failed_only mode with ``stored`` already in the Variable."""
    key = _key()
    store = FakeStore({key: list(stored)})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        store=store,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
        **kwargs,
    )
    return op, runner, store, key


def _targets(runner):
    return runner.calls[0]["test_path"]


@pytest.mark.parametrize(
    "poison",
    [
        "-p",
        "-pevil_module",
        "--rootdir=/tmp/pwn",
        "-c",
        "--import-mode=importlib",
        "-x",
    ],
)
def test_option_like_entry_never_reaches_pytest(poison):
    op, runner, _store, _key_ = _op([poison])
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    targets = _targets(runner)
    print(f"[inject:{poison}] targets={targets!r}")
    assert poison not in targets


def test_plugin_load_payload_is_neutralised():
    # The concrete escalation: -p <module> makes pytest import arbitrary code.
    # Both tokens must go -- the flag AND its orphaned value, neither of which
    # is a node-id -- leaving nothing usable, so the full suite runs instead.
    op, runner, _store, _key_ = _op(["-p", "evil_module"])
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    targets = _targets(runner)
    print(f"[inject:plugin] targets={targets!r}")
    assert targets == "tests/"


def test_all_poisoned_falls_back_to_the_full_suite():
    # Nothing usable left -> run everything, never a narrowed poisoned set.
    op, runner, _store, _key_ = _op(["-p", "--rootdir=/tmp/pwn"])
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    targets = _targets(runner)
    print(f"[inject:all-poison] targets={targets!r}")
    assert targets == "tests/"


def test_legit_node_ids_still_narrow_the_run():
    # The guard must not break the actual feature.
    op, runner, _store, _key_ = _op(["tests.test_x::test_a"])
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    targets = _targets(runner)
    print(f"[inject:legit] targets={targets!r}")
    assert targets == ["tests/test_x.py::test_a"]


def test_mixed_set_keeps_only_the_legit_ids():
    op, runner, _store, _key_ = _op(
        ["tests.test_x::test_a", "-p", "evil_module", "tests.test_y::test_b"]
    )
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    targets = _targets(runner)
    print(f"[inject:mixed] targets={targets!r}")
    assert targets == ["tests/test_x.py::test_a", "tests/test_y.py::test_b"]
    assert not any(t.startswith("-") for t in targets)


def test_tampered_variable_is_still_consumed():
    # Consume-on-read must still happen, or the poison survives every retry.
    op, runner, store, key = _op(["-p", "evil_module"])
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    print(f"[inject:consumed] deletes={store.deletes!r}")
    assert key in store.deletes


def test_tampering_is_logged(caplog):
    op, runner, _store, _key_ = _op(["tests.test_x::test_a", "-p"])
    with caplog.at_level(logging.WARNING):
        op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    print(f"[inject:log] {caplog.text!r}")
    assert "failed_only" in caplog.text
    assert "node-id" in caplog.text


def test_bare_value_without_separator_is_rejected():
    # An orphaned option value ("evil_module") is not a node-id either: without
    # "::" it would otherwise reach pytest as an arbitrary path target.
    op, runner, _store, _key_ = _op(["evil_module", "/etc"])
    op.execute(_ctx(dag_id="d", task_id="t", run_id="r"))
    targets = _targets(runner)
    print(f"[inject:bare-value] targets={targets!r}")
    assert targets == "tests/"
