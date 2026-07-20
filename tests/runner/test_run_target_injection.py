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


"""Runner-level guard: positional targets are never allowed to be pytest options.

The runner is the last gate before the child process, so it refuses option-like
targets whatever the caller did. These are end-to-end: a leaked "-p evil" would
make the real pytest abort on the plugin import. Shared fakes in _run_helpers."""

from __future__ import annotations

import pytest
from _run_helpers import _run, _suite

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_option_like_target_is_dropped_and_the_run_still_works(tmp_path):
    # If "-p nonexistent_plugin" leaked through, pytest would die importing it.
    suite = _suite(tmp_path, "def test_one(): assert True\n")
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = _run(
        runner,
        ["-pdefinitely_not_a_real_plugin_xyz", suite],
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(f"[target:dropped] exit={artifacts.exit_code} total={result.total}")
    assert artifacts.exit_code == 0
    assert result.total == 1


def test_long_option_target_is_dropped(tmp_path):
    suite = _suite(tmp_path, "def test_one(): assert True\n")
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = _run(
        runner,
        [suite, "--rootdir=/nonexistent/pwn"],
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(f"[target:long-opt] exit={artifacts.exit_code} total={result.total}")
    assert artifacts.exit_code == 0
    assert result.total == 1


def test_only_option_like_targets_is_rejected():
    # Nothing usable left -> fail closed rather than let pytest collect the cwd.
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    with pytest.raises(TestExecutionError, match="test_path"):
        _run(runner, ["-p", "--rootdir=/x"], report_request=parser.report_request)


def test_orphaned_option_value_degrades_to_a_plain_path(tmp_path):
    # Dropping "-p" leaves its value behind as a positional. The runner cannot
    # tell an option's value from a target, so this is deliberate: the result is
    # a nonexistent *path* (pytest exits 4), never a loaded plugin.
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = _run(
        runner,
        ["-p", "definitely_not_a_real_plugin_xyz"],
        pytest_args=[],
        report_request=parser.report_request,
    )
    print(
        f"[target:orphan] exit={artifacts.exit_code} stderr={artifacts.stderr[:120]!r}"
    )
    assert artifacts.exit_code == 4
    assert "Error importing plugin" not in artifacts.stderr


def test_option_like_target_is_logged(tmp_path, caplog):
    suite = _suite(tmp_path, "def test_one(): assert True\n")
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    with caplog.at_level("WARNING"):
        _run(
            runner, [suite, "-x"], pytest_args=[], report_request=parser.report_request
        )
    print(f"[target:log] {caplog.text!r}")
    assert "option" in caplog.text.lower()


def test_leading_dash_only_matters_at_position_zero(tmp_path):
    # A path that merely CONTAINS a dash is perfectly legitimate.
    d = tmp_path / "my-tests"
    d.mkdir()
    f = d / "test_dash.py"
    f.write_text("def test_one(): assert True\n")
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = _run(
        runner, [str(f)], pytest_args=[], report_request=parser.report_request
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(f"[target:dash-in-name] exit={artifacts.exit_code} total={result.total}")
    assert artifacts.exit_code == 0
    assert result.total == 1
