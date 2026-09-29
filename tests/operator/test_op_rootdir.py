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

"""Reruns through the REAL runner and parser when pytest's rootdir is not the
worker's cwd -- e.g. ``test_path="/opt/airflow/tests"`` run from ``/opt/airflow``.

The report's node-ids are relative to the rootdir; handed back verbatim they
were "not found" from the worker's cwd, so rerun_failed / failed_only re-ran
nothing and failed the task with "0 failed out of 0". And without a pytest
config file a narrowed run picks a smaller rootdir of its own: the suite's top
conftest.py stops loading and the next round's ids change base. The fakes
elsewhere can't see any of this: it lives in the seam between the parser's ids
and the runner's paths.
"""

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

from airflow_pytest_operator.exceptions import TestExecutionError, TestsFailedError
from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator

# A fixture from the suite's top conftest.py, used by a test one directory
# below it that fails on its first FAILS runs (the counter file sits next to it).
_CONFTEST = """\
import pytest

@pytest.fixture
def answer():
    return 42
"""
_FLAKY_TEST = """\
import pathlib

FAILS = {fails}

def test_stable(answer):
    assert answer == 42

def test_flaky(answer):
    counter = pathlib.Path(__file__).with_name("flaky.runs")
    runs = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(runs + 1))
    assert runs >= FAILS, "still failing"
"""
_FLAKY_ID = "sub.test_flaky::test_flaky"


def _suite(root, *, fails=1, ini=False):
    """root/tests/{conftest.py, sub/test_flaky.py} (+ root/pytest.ini)."""
    tests = root / "tests"
    (tests / "sub").mkdir(parents=True)
    (tests / "conftest.py").write_text(_CONFTEST)
    (tests / "sub" / "test_flaky.py").write_text(_FLAKY_TEST.format(fails=fails))
    if ini:
        (root / "pytest.ini").write_text("[pytest]\n")
    return tests


def _flaky_runs(tests):
    return int((tests / "sub" / "flaky.runs").read_text())


@pytest.fixture
def worker_cwd(tmp_path, monkeypatch):
    cwd = tmp_path / "worker_cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd


@pytest.mark.parametrize("ini", [False, True], ids=["rootdir=tests", "rootdir=above"])
def test_rerun_failed_recovers_from_another_cwd(tmp_path, worker_cwd, ini):
    tests = _suite(tmp_path, ini=ini)
    op = PytestOperator(task_id="t", test_path=str(tests), rerun_failed=1)

    out = op.execute(_ctx())

    print(f"[rootdir:rerun ini={ini}] {out}")
    assert _flaky_runs(tests) == 2  # the rerun really ran it
    assert out["success"] is True
    assert out["still_failing_node_ids"] == []
    assert len(out["recovered_node_ids"]) == 1


def test_rerun_rounds_keep_the_conftest_and_the_ids(tmp_path, worker_cwd):
    # Round 1 still fails, round 2 recovers. Each narrowed round must load the
    # top conftest's fixture and report ids on the full run's base -- or round 1
    # errors on a missing fixture and round 2 cannot find what round 1 reported.
    tests = _suite(tmp_path, fails=2)
    op = PytestOperator(task_id="t", test_path=str(tests), rerun_failed=2)

    out = op.execute(_ctx())

    print(f"[rootdir:two_rounds] {out}")
    assert _flaky_runs(tests) == 3
    assert out["rerun_rounds"] == 2
    assert out["recovered_node_ids"] == [_FLAKY_ID]
    assert (out["success"], out["still_failing_node_ids"]) == (True, [])


def test_failed_only_retry_runs_only_the_failure_from_another_cwd(tmp_path, worker_cwd):
    tests = _suite(tmp_path)
    store = FakeStore()
    ids = {"dag_id": "d", "task_id": "t", "run_id": "r", "max_tries": 1}

    def attempt(try_number):
        op = PytestOperator(
            task_id="t",
            test_path=str(tests),
            test_retry_strategy="failed_only",
            store=store,
        )
        return op.execute(_ctx(try_number, **ids))

    with pytest.raises(TestsFailedError):
        attempt(1)
    assert store.data[_key()] == [_FLAKY_ID]

    out = attempt(2)  # needs the top conftest's fixture, too

    print(f"[rootdir:failed_only] {out}")
    assert (out["total"], out["passed"]) == (1, 1)  # narrowed, not the full suite
    assert out["failed_node_ids"] == []
    assert _flaky_runs(tests) == 2
    assert _key() not in store.data  # consumed, nothing orphaned


def test_failed_only_narrows_the_same_way_on_every_retry(tmp_path, worker_cwd):
    # Attempt 2 narrows, still fails, and hands the failure to attempt 3. Its
    # ids must stay on the full run's base -- and its narrowed run must still
    # see the suite's top conftest, or the rerun errors instead of failing.
    tests = _suite(tmp_path, fails=2)
    store = FakeStore()
    ids = {"dag_id": "d", "task_id": "t", "run_id": "r", "max_tries": 2}

    def attempt(try_number):
        op = PytestOperator(
            task_id="t",
            test_path=str(tests),
            test_retry_strategy="failed_only",
            store=store,
        )
        return op.execute(_ctx(try_number, **ids))

    for try_number in (1, 2):
        with pytest.raises(TestsFailedError) as exc:
            attempt(try_number)
        print(f"[rootdir:chain {try_number}] {exc.value} stored={store.data[_key()]}")
        assert exc.value.result.errors == 0  # the fixture was there, not missing
        assert store.data[_key()] == [_FLAKY_ID]  # same id every attempt

    out = attempt(3)

    assert (out["total"], out["passed"]) == (1, 1)
    assert _flaky_runs(tests) == 3


@pytest.mark.parametrize("fail_on_test_failure", [False, True])
def test_a_collection_error_is_not_rerun_and_stays_failing(
    tmp_path, worker_cwd, fail_on_test_failure
):
    # A module that cannot import has no "::test" id to select: it is not
    # re-run (which used to exit 4) and never counts as recovered.
    tests = _suite(tmp_path)
    (tests / "test_broken.py").write_text("import no_such_module_xyz\n")
    op = PytestOperator(
        task_id="t",
        test_path=str(tests),
        rerun_failed=1,
        fail_on_test_failure=fail_on_test_failure,
        pytest_args=["--continue-on-collection-errors"],
    )

    if fail_on_test_failure:
        # The last round is green; the error must still name what failed.
        with pytest.raises(TestsFailedError) as exc:
            op.execute(_ctx())
        assert exc.value.result.errors == 1
        return
    out = op.execute(_ctx())

    print(f"[rootdir:collection_error] {out}")
    assert out["success"] is False
    assert out["recovered_node_ids"] == [_FLAKY_ID]
    assert out["still_failing_node_ids"] == ["test_broken"]


def test_failed_only_retry_rechecks_a_module_that_could_not_be_imported(
    tmp_path, worker_cwd
):
    # The worst outcome for a test operator: a green task. A module that failed
    # to import is stored without a "::test" part, so a narrowed retry could
    # never re-check it -- and would pass once the other failure recovered.
    tests = _suite(tmp_path)
    (tests / "test_broken.py").write_text("import no_such_module_xyz\n")
    store = FakeStore()
    ids = {"dag_id": "d", "task_id": "t", "run_id": "r", "max_tries": 1}

    def attempt(try_number):
        op = PytestOperator(
            task_id="t",
            test_path=str(tests),
            test_retry_strategy="failed_only",
            store=store,
            pytest_args=["--continue-on-collection-errors"],
        )
        return op.execute(_ctx(try_number, **ids))

    with pytest.raises(TestsFailedError):
        attempt(1)
    assert store.data[_key()] == ["test_broken", _FLAKY_ID]

    with pytest.raises(TestsFailedError) as exc:
        attempt(2)

    print(f"[rootdir:broken_module] {exc.value}")
    assert exc.value.result.errors == 1  # the broken module ran again
    assert exc.value.result.total == 3  # the full suite, not the narrowed set


def test_failed_only_with_a_vanished_file_runs_the_full_suite(tmp_path, worker_cwd):
    # Narrowing to the rest would never re-check the missing one.
    tests = _suite(tmp_path, fails=0)
    store = FakeStore({_key(): [_FLAKY_ID, "sub.test_renamed::test_x"]})
    op = PytestOperator(
        task_id="t",
        test_path=str(tests),
        test_retry_strategy="failed_only",
        store=store,
    )

    out = op.execute(_ctx(2, dag_id="d", task_id="t", run_id="r", max_tries=1))

    print(f"[rootdir:vanished] {out}")
    assert out["total"] == 2  # the whole suite ran


def test_an_explicit_rootdir_is_not_overridden(tmp_path, worker_cwd):
    tests = _suite(tmp_path)
    runner = FakeRunner(RunArtifacts(exit_code=1, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path=str(tests),
        rerun_failed=1,
        pytest_args=["--rootdir=/mine"],
        fail_on_test_failure=False,
        runner=runner,
        parser=SequenceParser([_res([_FLAKY_ID], passed=1), _res(passed=1)]),
    )
    op.execute(_ctx())
    rerun_args = runner.calls[1]["pytest_args"]
    assert rerun_args.count("--rootdir=/mine") == 1
    assert "--rootdir" not in rerun_args


def test_usage_error_is_an_execution_error_not_a_test_failure(tmp_path, worker_cwd):
    # pytest exits 4 on a target it cannot find, after writing an empty report.
    # That used to read as "0 failed out of 0" -- and pass with
    # fail_on_test_failure=False.
    op = PytestOperator(
        task_id="t",
        test_path="missing.py::test_x",
        fail_on_test_failure=False,
    )
    with pytest.raises(TestExecutionError) as exc:
        op.execute(_ctx())
    print(f"[rootdir:usage_error] {str(exc.value)[:120]!r}")
    assert "exit code 4" in str(exc.value)
    assert "not found" in str(exc.value)  # pytest's own stderr is surfaced


def test_usage_error_with_a_report_does_not_reach_the_parser():
    runner = FakeRunner(
        RunArtifacts(exit_code=4, report_path="/x.xml", stderr="ERROR: not found")
    )
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(task_id="t", test_path="suite/", runner=runner, parser=parser)
    with pytest.raises(TestExecutionError, match="usage error"):
        op.execute(_ctx())
    assert parser.parsed_paths == []


@pytest.mark.parametrize("shape", ["absolute", "dot_dot"])
def test_a_tampered_variable_cannot_run_a_file_outside_the_suite(
    tmp_path, worker_cwd, shape
):
    # Whoever can write the Variable controls pytest's positional arguments, and
    # pytest IMPORTS whatever file it is handed. The proof is the marker file:
    # the planted module never runs, and the attempt falls back to the suite.
    # (A symlink *inside* the suite is a different matter -- it is part of the
    # suite, so even a plain full run collects through it; reselect still
    # refuses to re-select through one, see test_reselect.py.)
    tests = _suite(tmp_path, fails=0)
    planted = tmp_path / "outside" / "test_evil.py"
    planted.parent.mkdir()
    marker = tmp_path / "executed"
    planted.write_text(
        f"import pathlib\npathlib.Path({str(marker)!r}).write_text('x')\n"
    )
    stored = (
        f"{planted}::test_evil"
        if shape == "absolute"
        else "../outside/test_evil.py::test_evil"
    )

    op = PytestOperator(
        task_id="t",
        test_path=str(tests),
        test_retry_strategy="failed_only",
        store=FakeStore({_key(): [stored]}),
    )
    out = op.execute(_ctx(2, dag_id="d", task_id="t", run_id="r", max_tries=1))

    print(f"[rootdir:tamper {shape}] total={out['total']}")
    assert not marker.exists(), "the planted module was executed"
    assert out["total"] == 2  # the suite ran instead of the tampered selector
