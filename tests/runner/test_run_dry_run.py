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


"""dry_run / --collect-only at the runner level. Shared fakes in _run_helpers."""

from __future__ import annotations

import textwrap

import pytest

from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_dry_run_only_collects_does_not_execute_test_bodies(tmp_path):
    from airflow_pytest_operator.operators import PytestOperator

    marker = tmp_path / "test_body_executed"
    suite = tmp_path / "test_x.py"
    suite.write_text(
        textwrap.dedent(
            f"""
            import pathlib

            def test_would_fail():
                # Sentinel: prove whether the body ran.
                pathlib.Path({str(marker)!r}).touch()
                assert False, "this would fail if executed"

            def test_would_pass():
                pathlib.Path({str(marker)!r}).touch()
                assert True
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    op = PytestOperator(
        task_id="t",
        test_path=str(suite),
        dry_run=True,
        runner=runner,
    )

    summary = op.execute({})

    print(
        f"[dry_run:e2e] exit_code={summary['exit_code']} "
        f"failed={summary['failed']} "
        f"marker_exists={marker.exists()}"
    )

    # THE essential property: no test body ran.
    assert not marker.exists(), (
        "test body executed despite dry_run=True -- the operator's "
        "--collect-only flag was not honoured"
    )
    assert summary["exit_code"] == 0
    assert summary["failed"] == 0
    assert summary["errors"] == 0
    assert summary["total"] == 0


def test_dry_run_collection_error_surfaces_as_task_failure(tmp_path):
    from airflow_pytest_operator.exceptions import TestsFailedError
    from airflow_pytest_operator.operators import PytestOperator

    suite = tmp_path / "test_broken.py"
    suite.write_text("def test_x(:  # invalid syntax\n    pass\n")

    runner = SubprocessPytestRunner()
    op = PytestOperator(
        task_id="t",
        test_path=str(suite),
        dry_run=True,
        runner=runner,
    )

    with pytest.raises(TestsFailedError):
        op.execute({})
    print(
        "[dry_run:collection_error] dry_run with SyntaxError raised "
        "TestsFailedError as expected -- collection errors are NOT "
        "silenced by --collect-only"
    )


def test_dry_run_with_junit_parser_collects_but_lacks_count(tmp_path):
    from airflow_pytest_operator import JUnitResultParser
    from airflow_pytest_operator.operators import PytestOperator

    suite = tmp_path / "test_x.py"
    suite.write_text(
        textwrap.dedent(
            """
            def test_a(): assert True
            def test_b(): assert True
            def test_c(): assert True
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    op = PytestOperator(
        task_id="t",
        test_path=str(suite),
        dry_run=True,
        runner=runner,
        parser=JUnitResultParser(),
    )
    summary = op.execute({})

    print(f"[dry_run:junit_limitation] total={summary['total']}")
    # Collection succeeded (exit code 0, no test failed) ...
    assert summary["exit_code"] == 0
    assert summary["failed"] == 0
    # ... but JUnit can't tell us how many tests were collected.
    assert summary["total"] == 0
