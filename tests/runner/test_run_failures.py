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


"""Failure paths: unexpected exception during wait, on_kill mid-run. Shared fakes
in _run_helpers."""

from __future__ import annotations

import textwrap

import pytest
from _run_helpers import (
    _process_alive,
    _run,
)

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_unexpected_exception_during_wait_kills_subprocess(tmp_path, monkeypatch):
    import subprocess as _sp
    import time

    class FakeTaskTimeout(Exception):  # noqa: N818
        pass

    suite = tmp_path / "test_long.py"
    suite.write_text(
        textwrap.dedent(
            """
            import time
            def test_long_sleeper(): time.sleep(30)
            """
        ).strip()
    )

    original_popen = _sp.Popen
    captured_proc: dict = {}

    def patched_popen(*args, **kwargs):
        proc = original_popen(*args, **kwargs)
        captured_proc["proc"] = proc
        real_wait = proc.wait
        wait_call_count = {"n": 0}

        def evil_wait(timeout=None):
            wait_call_count["n"] += 1
            if wait_call_count["n"] == 1:
                time.sleep(0.6)
                raise FakeTaskTimeout("simulated Airflow execution_timeout")
            return real_wait(timeout=timeout)

        proc.wait = evil_wait  # type: ignore[method-assign]
        return proc

    monkeypatch.setattr(_sp, "Popen", patched_popen)

    runner = SubprocessPytestRunner()

    with pytest.raises(FakeTaskTimeout, match="simulated"):
        _run(runner, str(suite))

    proc = captured_proc["proc"]
    pid = proc.pid

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and _process_alive(pid):
        time.sleep(0.05)
    alive_after = _process_alive(pid)
    print(
        f"[unexpected_exc_kills_proc] pid={pid} alive_after_propagation={alive_after} "
        f"returncode={proc.returncode!r} "
        f"(alive_after must be False, returncode should be -SIGTERM=-15 or -SIGKILL=-9)"
    )

    assert not alive_after, (
        f"pytest subprocess (pid={pid}) survived the FakeTaskTimeout: this "
        "means _run_locked's `except BaseException -> _terminate` clause "
        "regressed, and Airflow's execution_timeout would now leak orphans."
    )
    assert proc.returncode in (-15, -9), (
        f"unexpected exit cause: returncode={proc.returncode}"
    )


def test_cancel_after_unexpected_exception_is_safe_noop(tmp_path, monkeypatch):
    import subprocess as _sp
    import time

    class FakeTaskTimeout(Exception):  # noqa: N818
        pass

    suite = tmp_path / "test_long.py"
    suite.write_text(
        textwrap.dedent(
            """
            import time
            def test_long_sleeper(): time.sleep(30)
            """
        ).strip()
    )

    original_popen = _sp.Popen

    def patched_popen(*args, **kwargs):
        proc = original_popen(*args, **kwargs)
        real_wait = proc.wait
        wait_calls = {"n": 0}

        def evil_wait(timeout=None):
            wait_calls["n"] += 1
            if wait_calls["n"] == 1:
                time.sleep(0.4)
                raise FakeTaskTimeout("airflow-style timeout")
            return real_wait(timeout=timeout)

        proc.wait = evil_wait  # type: ignore[method-assign]
        return proc

    monkeypatch.setattr(_sp, "Popen", patched_popen)

    runner = SubprocessPytestRunner()
    with pytest.raises(FakeTaskTimeout):
        _run(runner, str(suite))

    print(
        "[cancel_after_unexpected_exc] calling cancel() post-exception, "
        "expecting silent no-op"
    )
    runner.cancel()
    runner.cancel()
    print("[cancel_after_unexpected_exc] cancel() completed without error")


def test_on_kill_during_active_run_kills_subprocess(tmp_path):
    import threading
    import time

    from airflow_pytest_operator.operators import PytestOperator

    suite = tmp_path / "test_long.py"
    suite.write_text(
        textwrap.dedent(
            """
            import time
            def test_long(): time.sleep(30)
            """
        ).strip()
    )

    runner = SubprocessPytestRunner()
    op = PytestOperator(task_id="t", test_path=str(suite), runner=runner)

    captured_pid: dict = {}

    def watcher():
        for _ in range(50):
            time.sleep(0.1)
            proc = runner._proc  # noqa: SLF001 — test introspection
            if proc is not None:
                captured_pid["pid"] = proc.pid
                return

    def killer():
        time.sleep(1.0)
        op.on_kill()

    threading.Thread(target=watcher, daemon=True).start()
    kill_thread = threading.Thread(target=killer, daemon=True)
    kill_thread.start()

    started = time.monotonic()
    with pytest.raises(TestExecutionError):
        op.execute({})
    elapsed = time.monotonic() - started

    kill_thread.join(timeout=3.0)

    pid = captured_pid.get("pid")
    assert pid is not None, "watcher never saw a live child PID"

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and _process_alive(pid):
        time.sleep(0.05)
    alive_after = _process_alive(pid)
    print(
        f"[on_kill_during_run] pid={pid} elapsed={elapsed:.2f}s "
        f"alive_after_on_kill={alive_after} (must be False)"
    )

    # Cleanup also ran: PytestOperator.on_kill() calls
    # self._runner.cleanup(success=False) defensively. We don't assert
    # the directory state here (the operator's own finally also runs and
    # cleanup is idempotent under lock), only that no exception leaked.
    assert not alive_after, (
        f"on_kill failed to terminate pytest subprocess (pid={pid}); "
        "Airflow's SIGTERM path would leave an orphan."
    )
    # Sanity: the run did NOT linger past the kill -- elapsed should be
    # well under the test_long's 30s, demonstrating the kill actually
    # short-circuited the wait.
    assert elapsed < 10.0, (
        f"execute() took {elapsed:.2f}s -- on_kill should short-circuit "
        "the wait, but the test ran far too long."
    )
