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


"""dry_run: --collect-only injection, its spelling aliases, and pytest_args
immutability. Shared fakes in _op_helpers."""

from __future__ import annotations

from _op_helpers import (
    FakeParser,
    FakeRunner,
    _ctx,
    _result,
)

from airflow_pytest_operator.models import RunArtifacts
from airflow_pytest_operator.operators import PytestOperator


def test_dry_run_appends_collect_only_to_runner_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dry_run:args] forwarded pytest_args = {forwarded_args!r}")

    assert forwarded_args[:2] == ["-k", "smoke"]
    assert forwarded_args.count("--collect-only") == 1
    assert forwarded_args[-1] == "--collect-only"


def test_dry_run_false_does_not_append_collect_only():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=1))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke"],
        # dry_run=False is the default; explicit to make intent clear
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dry_run:default_off] forwarded pytest_args = {forwarded_args!r}")
    assert "--collect-only" not in forwarded_args
    assert forwarded_args == ["-k", "smoke"]


def test_dry_run_does_not_mutate_user_pytest_args():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    user_args = ["-k", "smoke", "-v"]
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=user_args,
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())
    op.execute(_ctx())  # second execute (retry simulation)

    print(f"[dry_run:no_mutation] op.pytest_args after two runs = {op.pytest_args!r}")
    assert op.pytest_args == ["-k", "smoke", "-v"]
    for call in runner.calls:
        assert call["pytest_args"].count("--collect-only") == 1


def test_dry_run_default_is_false():
    op = PytestOperator(task_id="t", test_path="tests/")
    print(f"[dry_run:default_pin] op.dry_run = {op.dry_run}")
    assert op.dry_run is False


def test_dry_run_logs_indicate_mode():
    # Airflow operator loggers do NOT always propagate to root, so pytest's
    # ``caplog`` fixture misses them on some Airflow versions. We capture
    # at the source: mock op.log.info and inspect what was called. This
    # mirrors the pattern used by test_stdout_and_stderr_are_logged.
    from unittest import mock

    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    with mock.patch.object(op.log, "info") as info:
        op.execute(_ctx())

    # Flatten all info() calls into one string so users searching for
    # either "dry-run" or "--collect-only" find the matching line.
    logged = " ".join(str(c) for c in info.call_args_list)
    print(f"[dry_run:log] info() calls: {logged!r}")
    assert "dry-run" in logged
    assert "--collect-only" in logged


def test_dry_run_does_not_double_add_when_user_passed_collect_only():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["-k", "smoke", "--collect-only"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:explicit] forwarded = {forwarded_args!r}")

    assert forwarded_args.count("--collect-only") == 1
    assert forwarded_args == ["-k", "smoke", "--collect-only"]


def test_dry_run_recognises_collectonly_alias():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--collectonly"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:legacy_alias] forwarded = {forwarded_args!r}")
    # Operator left the user's alias in place AND did not append its own
    # --collect-only on top.
    assert "--collect-only" not in forwarded_args
    assert forwarded_args == ["--collectonly"]


def test_dry_run_recognises_co_short_alias():
    # ``--co`` is the short alias. Same dedup principle.
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--co"],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:short_alias] forwarded = {forwarded_args!r}")
    assert "--collect-only" not in forwarded_args
    assert forwarded_args == ["--co"]


def test_dry_run_dedup_does_not_touch_other_repeated_flags():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=[
            "-v",
            "-v",  # extra-verbose
            "-o",
            "console_output_style=count",  # paired -o #1
            "-o",
            "junit_family=xunit2",  # paired -o #2
            "--ignore=tests/slow",  # ignore #1
            "--ignore=tests/flaky",  # ignore #2
        ],
        dry_run=True,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:narrow] forwarded = {forwarded_args!r}")
    expected_user_args = [
        "-v",
        "-v",
        "-o",
        "console_output_style=count",
        "-o",
        "junit_family=xunit2",
        "--ignore=tests/slow",
        "--ignore=tests/flaky",
    ]
    # User's args, in order, followed by our appended --collect-only.
    assert forwarded_args[:-1] == expected_user_args
    assert forwarded_args[-1] == "--collect-only"
    # Total count: 8 user args + 1 appended --collect-only = 9.
    assert len(forwarded_args) == 9


def test_dry_run_false_with_collect_only_in_args_still_runs_collection():
    runner = FakeRunner(RunArtifacts(exit_code=0, report_path="/x.xml"))
    parser = FakeParser(_result(passed=0))
    op = PytestOperator(
        task_id="t",
        test_path="tests/",
        pytest_args=["--collect-only", "-k", "smoke"],
        dry_run=False,
        runner=runner,
        parser=parser,
    )

    op.execute(_ctx())

    forwarded_args = runner.calls[0]["pytest_args"]
    print(f"[dedup:user_explicit_dry_run_off] forwarded = {forwarded_args!r}")
    assert forwarded_args == ["--collect-only", "-k", "smoke"]
