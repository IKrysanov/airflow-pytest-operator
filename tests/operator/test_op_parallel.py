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


"""parallel / dist: drive pytest-xdist from the operator (-n / --dist) on the first
full run, deferring to explicit user args. Shared fakes in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    FakeStore,
    SequenceParser,
    _ctx,
    _key,
    _res,
    _result,
)

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_parallel_dist_default_none():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[parallel:default] parallel={op.parallel!r} dist={op.dist!r}")
    assert op.parallel is None
    assert op.dist is None


def test_parallel_int_appends_n_to_runner_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        parallel=4,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:int] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "smoke", "-n", "4"]
    assert "--dist" not in forwarded


def test_parallel_auto_keyword():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel="auto",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:auto] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "auto"]


def test_dist_appends_mode_after_n():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel=4,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[dist:mode] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "4", "--dist", "loadscope"]


def test_parallel_none_adds_nothing():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:none] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "smoke"]
    assert "-n" not in forwarded


def test_parallel_skipped_in_dry_run():
    # dry-run runs no test bodies, so workers would only add startup latency;
    # --collect-only is still appended, -n is not.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel=4,
        dist="loadscope",
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:dry_run] forwarded = {forwarded!r}")
    assert "--collect-only" in forwarded
    assert "-n" not in forwarded
    assert "--dist" not in forwarded


def test_parallel_defers_to_explicit_n_separate():
    # User already drives -n via pytest_args -> operator must not add a second
    # one, and must skip --dist too (the user owns parallelism).
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-n", "8"],
        parallel=4,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:defer_separate] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "8"]
    assert forwarded.count("-n") == 1
    assert "--dist" not in forwarded


def test_parallel_defers_to_explicit_n_equals_form():
    # The "-n=8" and "-nN" spellings are detected too, not only the split form.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-n=8"],
        parallel=4,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:defer_equals] forwarded = {forwarded!r}")
    assert forwarded == ["-n=8"]


def test_parallel_not_applied_to_in_process_reruns():
    # The full run is parallelised; the in-process rerun_failed round re-runs a
    # couple of node-ids serially (uses list(self.pytest_args), never -n).
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first full run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel=4,
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    first = runner.calls[0]["pytest_args"]
    rerun = runner.calls[1]["pytest_args"]
    print(f"[parallel:reruns] first={first!r} rerun={rerun!r}")
    assert len(runner.calls) == 2
    assert first == ["-n", "4"]  # full run parallelised
    assert "-n" not in rerun  # rerun stays serial


def test_parallel_does_not_mutate_user_pytest_args():
    # Like dry-run's --collect-only injection: the user's list is never mutated,
    # and a retry (second execute) appends exactly one -n, not two.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    user_args = ["-k", "smoke"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        parallel=2,
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    op.execute(_ctx())  # retry simulation
    print(f"[parallel:no_mutation] op.pytest_args = {op.pytest_args!r}")
    assert op.pytest_args == ["-k", "smoke"]
    for call in runner.calls:
        assert call["pytest_args"].count("-n") == 1


def test_parallel_zero_raises_value_error():
    with pytest.raises(ValueError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel=0)


def test_parallel_bool_raises_type_error():
    # bool is an int subclass; True must not slip through as a worker count.
    with pytest.raises(TypeError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel=True)


def test_parallel_bad_string_raises_value_error():
    with pytest.raises(ValueError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel="lots")


def test_parallel_bad_type_raises_type_error():
    with pytest.raises(TypeError, match="parallel"):
        PytestOperator(task_id="t", test_path="tests/", parallel=2.5)


def test_dist_invalid_mode_raises_value_error():
    with pytest.raises(ValueError, match="dist"):
        PytestOperator(task_id="t", test_path="tests/", parallel=2, dist="bogus")


def test_dist_without_parallel_raises_value_error():
    # --dist is inert without -n; reject it rather than silently no-op.
    with pytest.raises(ValueError, match="dist requires parallel"):
        PytestOperator(task_id="t", test_path="tests/", dist="loadscope")


def test_dist_valid_with_parallel_is_accepted():
    op = PytestOperator(
        task_id="t", test_path="tests/", parallel="auto", dist="worksteal"
    )
    assert op.parallel == "auto"
    assert op.dist == "worksteal"


def test_dist_defers_to_explicit_dist_in_pytest_args():
    # User drives --dist via pytest_args but NOT -n: deference is keyed on -n,
    # so the operator still adds -n -- but it must NOT add a second --dist
    # (xdist's argparse keeps the last one, silently dropping the user's mode).
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--dist", "loadfile"],
        parallel=4,
        dist="load",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[dist:defer_split] forwarded = {forwarded!r}")
    assert forwarded == ["--dist", "loadfile", "-n", "4"]
    assert forwarded.count("--dist") == 1  # the user's mode survives


def test_dist_defers_to_explicit_dist_equals_form():
    # The "--dist=loadfile" spelling is detected too, not only the split form.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--dist=loadfile"],
        parallel=4,
        dist="load",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[dist:defer_equals] forwarded = {forwarded!r}")
    assert forwarded == ["--dist=loadfile", "-n", "4"]
    assert forwarded.count("--dist") == 0  # only the equals spelling present


def test_parallel_is_applied_to_failed_only_retry():
    # Unlike the in-process rerun_failed rounds (which stay serial), a
    # failed_only Airflow retry runs the narrowed set through effective_args, so
    # -n / --dist DO apply -- the narrowed set can still be large.
    key = _key()
    store = FakeStore({key: ["tests.test_x::test_a"]})
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        test_retry_strategy="failed_only",
        parallel=4,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_res([], passed=1)),
        store=store,
    )
    op.execute(_ctx(try_number=2, dag_id="d", task_id="t", run_id="r"))
    forwarded = runner.calls[0]["pytest_args"]
    target = runner.calls[0]["test_path"]
    print(f"[parallel:failed_only] target={target!r} forwarded={forwarded!r}")
    assert target == ["tests/test_x.py::test_a"]  # narrowed to the prior failure
    assert forwarded == ["-n", "4", "--dist", "loadscope"]  # parallelism applied
