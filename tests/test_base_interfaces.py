"""Tests for the abstract Runner/Parser interface defaults.

The base classes provide default no-op ``cancel``/``cleanup`` so that
simple runners stay Liskov-substitutable without reimplementing them,
and the abstract ``run``/``parse`` bodies raise ``NotImplementedError``
as an explicit contract marker. We exercise those defaults directly
through a minimal concrete subclass.
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

import pytest

from airflow_pytest_operator.models import RunArtifacts, TestRunResult
from airflow_pytest_operator.reporters.base import ResultParser
from airflow_pytest_operator.runners.base import PytestRunner


class _MinimalRunner(PytestRunner):
    """Implements only the abstract ``run``; inherits default cancel/cleanup."""

    def run(self, test_path, *, pytest_args=None, env=None):
        return RunArtifacts(exit_code=0, junit_xml_path=None)


class _MinimalParser(ResultParser):
    def parse(self, report_path, *, exit_code=0):
        return TestRunResult(
            total=0,
            passed=0,
            failed=0,
            skipped=0,
            errors=0,
            duration=0.0,
            exit_code=exit_code,
        )


def test_default_cancel_is_noop_and_safe():
    runner = _MinimalRunner()
    # Default cancel() returns None and never raises (substitutability).
    assert runner.cancel() is None


def test_default_cleanup_is_noop_and_safe():
    runner = _MinimalRunner()
    assert runner.cleanup(success=True) is None
    assert runner.cleanup(success=False) is None


def test_minimal_runner_run_works():
    runner = _MinimalRunner()
    artifacts = runner.run("tests/")
    assert artifacts.exit_code == 0
    assert artifacts.junit_xml_path is None


def test_minimal_parser_parse_works():
    parser = _MinimalParser()
    result = parser.parse("/some/report.xml", exit_code=0)
    assert result.total == 0
    assert result.success is True


def test_abstract_classes_cannot_be_instantiated_directly():
    # Both interfaces declare abstract methods, so direct instantiation must
    # fail -- proving they are genuine ABCs, not accidentally concrete.
    with pytest.raises(TypeError):
        PytestRunner()  # type: ignore[abstract]
    with pytest.raises(TypeError):
        ResultParser()  # type: ignore[abstract]
