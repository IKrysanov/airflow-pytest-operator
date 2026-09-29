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

"""Copying a runner: Airflow deep-copies operators (DAG.partial_subset, and
dag.test() on Airflow 3.0), and the runner's threading.Lock used to make that
raise "cannot pickle '_thread.lock' object". Shared fakes in _run_helpers."""

from __future__ import annotations

import copy
import pickle

import pytest
from _run_helpers import _run, _suite

from airflow_pytest_operator.runners import SubprocessPytestRunner

_CONFIG = {
    "python_executable": "/usr/bin/python3",
    "timeout": 30,
    "cwd": "/srv/tests",
    "grace_period": 2.0,
    "cleanup": "on_success",
    "max_output_bytes": 1024,
    "verbose": True,
}


def _config(runner):
    return (
        runner._python,
        runner._timeout,
        runner._cwd,
        runner._grace_period,
        runner._cleanup,
        runner._max_output_bytes,
        runner._verbose,
    )


@pytest.mark.parametrize(
    "clone",
    [copy.deepcopy, lambda r: pickle.loads(pickle.dumps(r))],
    ids=["deepcopy", "pickle"],
)
def test_copy_keeps_the_configuration(clone):
    runner = SubprocessPytestRunner(**_CONFIG)
    clone_ = clone(runner)
    assert _config(clone_) == _config(runner)


def test_copy_is_an_idle_runner_that_owns_nothing():
    # A copy must not cancel the original's child or delete its temp dir.
    runner = SubprocessPytestRunner()
    runner._created_report_dir = "/tmp/owned-by-the-original"
    runner._kept_report_dir = "/data/reports"
    runner._proc = object()
    runner._running = True
    runner._cancelled = True

    clone = copy.deepcopy(runner)

    assert clone._created_report_dir is None
    assert clone._kept_report_dir is None
    assert clone._proc is None
    assert clone._running is False and clone._cancelled is False
    assert clone._lock is not runner._lock
    assert runner._created_report_dir == "/tmp/owned-by-the-original"


def test_copy_runs_independently_of_the_original(tmp_path):
    runner = SubprocessPytestRunner()
    path = _suite(tmp_path, "def test_ok(): pass\n")
    clone = copy.deepcopy(runner)
    assert _run(clone, path).exit_code == 0
    clone.cleanup()
    assert _run(runner, path).exit_code == 0
    runner.cleanup()


def test_run_state_attrs_cover_everything_init_run_state_sets():
    # A per-run field missing from _RUN_STATE_ATTRS would leak into copies; one
    # missing from the class-level declarations would be invisible to readers.
    blank = SubprocessPytestRunner.__new__(SubprocessPytestRunner)
    blank._init_run_state()
    run_state = set(SubprocessPytestRunner._RUN_STATE_ATTRS)
    assert set(vars(blank)) == run_state
    assert run_state <= set(SubprocessPytestRunner.__annotations__)
