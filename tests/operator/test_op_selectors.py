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


"""markers / keyword: ergonomic sugar for pytest's -m / -k selectors. Shared fakes
in _op_helpers."""

from __future__ import annotations

import pytest
from _op_helpers import (
    FakeParser,
    FakeRunner,
    SequenceParser,
    _ctx,
    _res,
    _result,
)

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_markers_keyword_default_none():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[sugar:default] markers={op.markers!r} keyword={op.keyword!r}")
    assert op.markers is None
    assert op.keyword is None


def test_markers_appends_dash_m():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke and not slow",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:markers] forwarded = {forwarded!r}")
    assert forwarded == ["-m", "smoke and not slow"]


def test_keyword_appends_dash_k():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        keyword="login or logout",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:keyword] forwarded = {forwarded!r}")
    assert forwarded == ["-k", "login or logout"]


def test_markers_and_keyword_together_after_user_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        markers="smoke",
        keyword="fast",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:both] forwarded = {forwarded!r}")
    assert forwarded == ["-x", "-m", "smoke", "-k", "fast"]


def test_markers_applies_in_dry_run_alongside_collect_only():
    # Selection narrows what gets collected -- useful for a scoped pre-flight.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke",
        dry_run=True,
        runner=runner,
        parser=FakeParser(_result(passed=0)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:dry_run] forwarded = {forwarded!r}")
    assert "-m" in forwarded and "smoke" in forwarded
    assert "--collect-only" in forwarded


def test_markers_defers_to_explicit_m_in_pytest_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-m", "regression"],
        markers="smoke",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:defer_m] forwarded = {forwarded!r}")
    assert forwarded == ["-m", "regression"]  # operator's markers not added
    assert forwarded.count("-m") == 1


def test_keyword_defers_to_concatenated_k_form():
    # The "-kfast" short concatenated spelling is detected, not only "-k fast".
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-kfast"],
        keyword="slow",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:defer_k] forwarded = {forwarded!r}")
    assert forwarded == ["-kfast"]


def test_empty_markers_is_skipped():
    # A Jinja template can render to "" at run time -> no flag added.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        markers="   ",  # whitespace-only, as a blank template would render
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:empty] forwarded = {forwarded!r}")
    assert forwarded == ["-x"]
    assert "-m" not in forwarded


def test_empty_keyword_is_skipped():
    # Symmetry with markers: a whitespace-only keyword (e.g. a template that
    # resolved to "") is skipped rather than passed as a blank -k selector.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        keyword="",  # empty, as a blank template would render
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[sugar:empty_keyword] forwarded = {forwarded!r}")
    assert forwarded == ["-x"]
    assert "-k" not in forwarded


def test_markers_not_applied_to_in_process_reruns():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = SequenceParser(
        [
            _res(["tests.test_x::test_a"], passed=2),  # first run: 1 failure
            _res([], passed=1),  # rerun: recovered
        ]
    )
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        markers="smoke",
        rerun_failed=1,
        runner=runner,
        parser=parser,
    )
    op.execute(_ctx())
    first = runner.calls[0]["pytest_args"]
    rerun = runner.calls[1]["pytest_args"]
    print(f"[sugar:reruns] first={first!r} rerun={rerun!r}")
    assert first == ["-m", "smoke"]
    assert "-m" not in rerun  # rerun targets explicit node-ids, no re-filtering


def test_markers_bad_type_raises_type_error():
    with pytest.raises(TypeError, match="markers"):
        PytestOperator(task_id="t", test_path="tests/", markers=["smoke"])


def test_keyword_bad_type_raises_type_error():
    with pytest.raises(TypeError, match="keyword"):
        PytestOperator(task_id="t", test_path="tests/", keyword=123)


def test_markers_keyword_do_not_mutate_user_pytest_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    user_args = ["-x"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        markers="smoke",
        keyword="fast",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    op.execute(_ctx())  # retry simulation
    print(f"[sugar:no_mutation] op.pytest_args = {op.pytest_args!r}")
    assert op.pytest_args == ["-x"]
    for call in runner.calls:
        assert call["pytest_args"].count("-m") == 1
        assert call["pytest_args"].count("-k") == 1


def test_has_flag_detects_all_spellings():
    # The helper now lives in operators/_constants.py; pin the spellings it
    # must recognise so the "defer to explicit user arg" logic stays correct.
    from airflow_pytest_operator.operators._constants import (
        DIST_FLAGS,
        NUMPROCESSES_FLAGS,
        has_flag,
    )

    assert has_flag(["-n", "4"], NUMPROCESSES_FLAGS)  # split
    assert has_flag(["-n=4"], NUMPROCESSES_FLAGS)  # equals
    assert has_flag(["-n4"], NUMPROCESSES_FLAGS)  # concatenated
    assert has_flag(["--numprocesses=4"], NUMPROCESSES_FLAGS)  # long equals
    assert not has_flag(["-k", "smoke"], NUMPROCESSES_FLAGS)  # unrelated flag
    assert not has_flag([], NUMPROCESSES_FLAGS)  # empty

    # --dist is long-only: the split and equals spellings match, and the
    # concatenated short-form heuristic must NOT fire for it.
    assert has_flag(["--dist", "loadscope"], DIST_FLAGS)  # split
    assert has_flag(["--dist=loadscope"], DIST_FLAGS)  # equals
    assert not has_flag(["-n", "4"], DIST_FLAGS)  # unrelated flag
    assert not has_flag(["--distribute"], DIST_FLAGS)  # no false concat match


def test_parallel_logical_keyword():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        parallel="logical",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[parallel:logical] forwarded = {forwarded!r}")
    assert forwarded == ["-n", "logical"]


def test_markers_and_parallel_compose_in_stable_order():
    # The two injection blocks (selectors, then xdist) must compose: user args,
    # then -m/-k, then -n/--dist.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-x"],
        markers="smoke",
        keyword="fast",
        parallel=2,
        dist="loadscope",
        runner=runner,
        parser=FakeParser(_result(passed=1)),
    )
    op.execute(_ctx())
    forwarded = runner.calls[0]["pytest_args"]
    print(f"[compose] forwarded = {forwarded!r}")
    assert forwarded == [
        "-x",
        "-m",
        "smoke",
        "-k",
        "fast",
        "-n",
        "2",
        "--dist",
        "loadscope",
    ]


def test_new_params_are_templated():
    # markers/keyword must be Jinja-rendered before execute(), like pytest_args.
    print(f"[template_fields] {PytestOperator.template_fields}")
    assert "markers" in PytestOperator.template_fields
    assert "keyword" in PytestOperator.template_fields
