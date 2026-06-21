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


"""A user-supplied --lf is forwarded verbatim and degrades safely. Shared fakes in
_run_helpers."""

from __future__ import annotations

from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_lf_with_empty_cache_falls_back_to_full_suite(tmp_path):
    # A user may still pass `--lf` themselves. With no prior `.pytest_cache` it
    # must NOT crash and must run the WHOLE suite (pytest's documented
    # fallback) -- the runner forwards the flag verbatim and does not interfere.
    path = _suite(
        tmp_path,
        """
        def test_a(): assert True
        def test_b(): assert False
        """,
    )
    cache = tmp_path / "fresh_cache"  # empty -> no "last failed" recorded
    artifacts = _run(
        SubprocessPytestRunner(),
        path,
        pytest_args=["--lf", "-o", f"cache_dir={cache}"],
    )
    print(f"exit_code={artifacts.exit_code}, report={artifacts.report_path!r}")
    assert artifacts.report_path is not None  # a report was produced -> no crash
    result = JUnitResultParser().parse(
        artifacts.report_path, exit_code=artifacts.exit_code
    )
    print(f"total={result.total} failed={result.failed}")
    assert result.total == 2  # fell back to the full suite, not zero/one test
    assert result.failed == 1
