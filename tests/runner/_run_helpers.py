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


"""Shared fakes and helpers for the split test modules."""

from __future__ import annotations

import importlib.util
import os
import textwrap
from pathlib import Path

import pytest

from airflow_pytest_operator.reporters import JUnitResultParser

_JUNIT_REPORT_REQUEST = JUnitResultParser().report_request


def _run(runner, *args, **kwargs):
    """Thin wrapper around ``runner.run`` that supplies the required
    ``report_request`` kwarg, so the body of each test stays focused on
    what it is actually testing (timeout, env, cwd, ...) rather than on
    the runner/parser plumbing.
    """
    kwargs.setdefault("report_request", _JUNIT_REPORT_REQUEST)
    return runner.run(*args, **kwargs)


def _suite(tmp_path: Path, src: str) -> str:
    f = tmp_path / "test_x.py"
    f.write_text(textwrap.dedent(src))
    return str(f)


def _process_alive(pid: int) -> bool:
    """Return True if the OS still has a live process with this PID.

    `os.kill(pid, 0)` is the POSIX idiom -- signal 0 does nothing, but
    the call still fails with OSError/ProcessLookupError if the process
    is gone (or with PermissionError if we lack rights, which is fine
    for our purposes: still alive).
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover -- not expected under test
        return True
    return True


requires_dotenv = pytest.mark.skipif(
    importlib.util.find_spec("dotenv") is None,
    reason="python-dotenv not installed (the optional [dotenv] extra)",
)


def _write_env(tmp_path: Path, content: str) -> str:
    p = tmp_path / "config.env"
    p.write_text(content)
    return str(p)
