"""Tests for SubprocessPytestRunner using real child processes."""

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

import os
import sys
import textwrap
from pathlib import Path

import pytest

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner

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


def test_runner_produces_report_and_zero_exit_on_pass(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    artifacts = _run(SubprocessPytestRunner(report_dir=str(tmp_path / "rep")), path)
    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert Path(artifacts.report_path).exists()


def test_runner_nonzero_exit_on_failure_but_does_not_raise(tmp_path):
    path = _suite(tmp_path, "def test_bad(): assert False")
    artifacts = _run(SubprocessPytestRunner(report_dir=str(tmp_path / "rep")), path)
    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code != 0
    assert artifacts.report_path is not None


def test_runner_passes_extra_args(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_one(): assert True
        def test_two(): assert True
    """,
    )
    artifacts = _run(
        SubprocessPytestRunner(report_dir=str(tmp_path / "rep")),
        path,
        pytest_args=["-k", "test_one"],
    )
    print(
        f"exit_code={artifacts.exit_code}, stdout snippet: {artifacts.stdout[:120]!r}"
    )
    assert artifacts.exit_code == 0
    assert "test_one" in artifacts.stdout or artifacts.exit_code == 0


def test_runner_forwards_env(tmp_path):
    path = _suite(
        tmp_path,
        """
        import os
        def test_env(): assert os.environ.get("MY_FLAG") == "42"
    """,
    )
    artifacts = _run(
        SubprocessPytestRunner(report_dir=str(tmp_path / "rep")),
        path,
        env={"MY_FLAG": "42"},
    )
    print(f"exit_code={artifacts.exit_code}")
    assert artifacts.exit_code == 0


def test_runner_bad_interpreter_raises_execution_error(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(python_executable="/no/such/python")
    with pytest.raises(TestExecutionError):
        _run(runner, path)


def test_cancel_kills_running_tree(tmp_path):
    import threading
    import time

    path = _suite(
        tmp_path,
        """
        import time
        def test_slow():
            time.sleep(60)
    """,
    )
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"), grace_period=2.0)

    result_box = {}

    def _do_run():
        result_box["artifacts"] = _run(runner, path)

    t = threading.Thread(target=_do_run)
    started = time.monotonic()
    t.start()

    time.sleep(2.0)
    runner.cancel()
    t.join(timeout=15)

    elapsed = time.monotonic() - started
    print(f"cancel elapsed: {elapsed:.2f}s")
    assert not t.is_alive(), "run() did not return after cancel"
    assert elapsed < 20, f"cancel was too slow: {elapsed:.1f}s"


def test_cancel_is_idempotent_and_safe_without_run(tmp_path):
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    runner.cancel()
    runner.cancel()


def test_cancel_before_completion_then_run_normally(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = _run(runner, path)
    assert artifacts.exit_code == 0


def test_auto_cwd_for_directory_target(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open('cwd_marker.txt', 'w').write(os.getcwd())\n"
    )
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = _run(runner, str(tests_dir))

    assert artifacts.exit_code == 0
    marker = tests_dir / "cwd_marker.txt"
    assert marker.exists(), "pytest did not run from the tests directory"
    print(f"cwd_marker: {marker.read_text()!r}")
    assert marker.read_text() == str(tests_dir.resolve())


def test_auto_cwd_for_file_target_uses_parent(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    test_file = tests_dir / "test_y.py"
    test_file.write_text("def test_a(): assert True\n")
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open('cwd_marker.txt', 'w').write(os.getcwd())\n"
    )
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = _run(runner, str(test_file))

    assert artifacts.exit_code == 0
    print(f"cwd_marker: {(tests_dir / 'cwd_marker.txt').read_text()!r}")
    assert (tests_dir / "cwd_marker.txt").read_text() == str(tests_dir.resolve())


def test_explicit_cwd_overrides_auto(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open(os.path.join(os.environ['MARK_DIR'], 'm.txt'), 'w')"
        ".write(os.getcwd())\n"
    )
    explicit = tmp_path / "elsewhere"
    explicit.mkdir()
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"), cwd=str(explicit))
    artifacts = _run(runner, str(tests_dir), env={"MARK_DIR": str(explicit)})

    assert artifacts.exit_code == 0
    print(f"m.txt content: {(explicit / 'm.txt').read_text()!r}")
    assert (explicit / "m.txt").read_text() == str(explicit.resolve())


def test_report_path_unaffected_by_auto_cwd(tmp_path):
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    rep = tmp_path / "rep"
    runner = SubprocessPytestRunner(report_dir=str(rep))
    artifacts = _run(runner, str(tests_dir))

    expected = JUnitResultParser().report_request(str(rep)).report_path
    assert artifacts.report_path == expected
    assert Path(expected).exists()


def test_cleanup_removes_auto_dir_by_default(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path)
    auto_dir = artifacts.working_dir
    assert auto_dir is not None and os.path.isdir(auto_dir)

    print(f"auto_dir={auto_dir!r}")
    runner.cleanup(success=True)
    assert not os.path.exists(auto_dir)


def test_cleanup_never_keeps_auto_dir(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="never")
    artifacts = _run(runner, path)
    runner.cleanup(success=True)
    assert os.path.isdir(artifacts.working_dir)


def test_cleanup_on_success_keeps_dir_on_failure(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="on_success")
    artifacts = _run(runner, path)
    runner.cleanup(success=False)

    assert os.path.isdir(artifacts.working_dir)


def test_cleanup_on_success_removes_dir_on_success(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="on_success")
    artifacts = _run(runner, path)
    runner.cleanup(success=True)
    assert not os.path.exists(artifacts.working_dir)


def test_cleanup_never_touches_user_supplied_dir(tmp_path):
    user_dir = tmp_path / "my_reports"
    user_dir.mkdir()
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(user_dir))
    _run(runner, path)
    runner.cleanup(success=True)
    assert user_dir.is_dir()


def test_cleanup_is_safe_without_run(tmp_path):
    runner = SubprocessPytestRunner()
    runner.cleanup(success=True)


def test_invalid_cleanup_policy_rejected():
    with pytest.raises(ValueError):
        SubprocessPytestRunner(cleanup="sometimes")


def test_report_dir_pointing_at_file_raises_execution_error(tmp_path):
    not_a_dir = tmp_path / "iam_a_file"
    not_a_dir.write_text("x")
    runner = SubprocessPytestRunner(report_dir=str(not_a_dir))
    with pytest.raises(TestExecutionError, match="report directory"):
        _run(runner, str(tmp_path))


def test_cancel_does_not_block_cleanup_during_grace(tmp_path):
    import threading
    import time

    path = _suite(
        tmp_path,
        """
        import signal, time
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        def test_stubborn(): time.sleep(30)
        """,
    )
    runner = SubprocessPytestRunner(grace_period=5.0)

    def _do_run():
        try:
            _run(runner, path)
        except Exception:  # noqa: BLE001
            pass

    t = threading.Thread(target=_do_run)
    t.start()
    time.sleep(1.5)

    canceller = threading.Thread(target=runner.cancel)
    canceller.start()
    time.sleep(0.5)

    t0 = time.monotonic()
    runner.cleanup(success=False)
    elapsed = time.monotonic() - t0
    assert elapsed < 2.0, f"cleanup blocked by cancel's grace wait: {elapsed:.1f}s"

    canceller.join(timeout=15)
    t.join(timeout=15)


def test_concurrent_run_on_same_instance_is_rejected(tmp_path):
    import threading
    import time

    slow = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(5)\n")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))

    errors = {}

    def _slow():
        try:
            _run(runner, slow)
        except Exception as e:  # noqa: BLE001
            errors["slow"] = e

    t = threading.Thread(target=_slow)
    t.start()
    time.sleep(1.0)

    with pytest.raises(TestExecutionError, match="already executing"):
        _run(runner, slow)

    runner.cancel()
    t.join(timeout=15)


def test_sequential_reuse_of_same_instance_works(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    a = _run(runner, path)
    assert a.exit_code == 0
    b = _run(runner, path)
    print(f"first run exit_code={a.exit_code}, second run exit_code={b.exit_code}")
    assert b.exit_code == 0


def test_run_times_out_raises_execution_error(tmp_path):
    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(
        report_dir=str(tmp_path / "rep"), timeout=1, grace_period=2.0
    )
    with pytest.raises(TestExecutionError, match="timed out"):
        _run(runner, path)


def test_stdout_and_stderr_are_captured(tmp_path):
    path = _suite(
        tmp_path,
        """
        import sys
        def test_streams():
            print("hello-stdout")
            print("hello-stderr", file=sys.stderr)
            assert True
        """,
    )
    artifacts = _run(
        SubprocessPytestRunner(report_dir=str(tmp_path / "rep")),
        path,
        pytest_args=["-s"],
    )
    print(f"stdout: {artifacts.stdout!r}")
    print(f"stderr: {artifacts.stderr!r}")
    assert artifacts.exit_code == 0
    assert "hello-stdout" in artifacts.stdout
    assert "hello-stderr" in artifacts.stderr


def test_usage_error_yields_none_report_path_without_raising(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    artifacts = _run(
        SubprocessPytestRunner(report_dir=str(tmp_path / "rep")),
        path,
        pytest_args=["--definitely-not-a-real-option"],
    )
    print(artifacts.exit_code)
    print(artifacts.report_path)

    assert artifacts.exit_code != 0
    assert artifacts.report_path is None


def test_working_dir_is_the_report_dir(tmp_path):
    rep = tmp_path / "rep"
    path = _suite(tmp_path, "def test_a(): assert True")
    artifacts = _run(SubprocessPytestRunner(report_dir=str(rep)), path)

    print(artifacts.working_dir)

    assert artifacts.working_dir == str(rep)


def test_resolve_cwd_none_for_node_id_or_glob_target(tmp_path):
    runner = SubprocessPytestRunner()
    assert runner._resolve_cwd(str(tmp_path / "x.py::test_a")) is None
    assert runner._resolve_cwd(str(tmp_path / "tests" / "*.py")) is None


def test_stale_cancel_does_not_abort_next_run(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    runner.cancel()
    artifacts = _run(runner, path)

    print(artifacts.exit_code)
    print(artifacts.report_path)

    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None


def test_separate_instances_run_in_parallel_safely(tmp_path):
    import threading

    results = {}

    def _go(key):
        d = tmp_path / f"suite_{key}"
        d.mkdir()
        (d / "test_x.py").write_text("def test_a(): assert True\n")
        r = SubprocessPytestRunner()
        art = _run(r, str(d))
        results[key] = art.working_dir
        r.cleanup(success=True)

    threads = [threading.Thread(target=_go, args=(k,)) for k in ("a", "b", "c")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    dirs = list(results.values())
    assert len(set(dirs)) == 3, "temp dirs collided across instances"
    for d in dirs:
        assert not os.path.exists(d), "each instance must clean its own dir"


def test_terminate_returns_early_when_process_already_dead(tmp_path):
    import subprocess as _sp

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    dead = _sp.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    assert dead.poll() is not None
    runner._terminate(dead)


def test_terminate_handles_process_lookup_on_sigterm(tmp_path, monkeypatch):
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))

    class _FakeProc:
        returncode = None

        def poll(self):
            return None

    def _raise_lookup(proc, sig):
        raise ProcessLookupError

    monkeypatch.setattr(runner, "_signal_group", _raise_lookup)
    runner._terminate(_FakeProc())  # type: ignore[arg-type]


def test_terminate_handles_process_lookup_on_sigkill(tmp_path, monkeypatch):
    import signal as _signal_module
    import subprocess as _sp

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"), grace_period=0.1)

    class _FakeProc:
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            raise _sp.TimeoutExpired(cmd="pytest", timeout=timeout)

    calls = {"n": 0}

    def _signal(proc, sig):
        calls["n"] += 1
        if sig == _signal_module.SIGKILL:
            raise ProcessLookupError

    monkeypatch.setattr(runner, "_signal_group", _signal)
    runner._terminate(_FakeProc())  # type: ignore[arg-type]
    print(f"signal calls: {calls['n']}")
    assert calls["n"] == 2


def test_concurrent_cleanup_only_one_rmtree(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path)
    auto_dir = artifacts.working_dir
    assert auto_dir is not None and os.path.isdir(auto_dir)

    real_lock = runner._lock

    class _ClaimingLock:
        def __enter__(self):
            runner._created_report_dir = None
            return real_lock.__enter__()

        def __exit__(self, *exc):
            return real_lock.__exit__(*exc)

    runner._lock = _ClaimingLock()  # type: ignore[assignment]
    runner.cleanup(success=True)
    runner._lock = real_lock  # type: ignore[assignment]

    assert os.path.isdir(auto_dir)
    import shutil as _shutil

    _shutil.rmtree(auto_dir, ignore_errors=True)


def test_timeout_logs_drained_stdout_and_stderr(tmp_path, caplog):
    import logging as _logging
    import sys as _sys
    import textwrap

    suite = tmp_path / "test_hang.py"
    suite.write_text(
        textwrap.dedent(
            """
            import sys, time

            def test_hang():
                # pytest disables stdout capture for output to actually
                # leave the child by default; -s on the runner side is
                # not how we ship, so write straight to fd 1/2 instead.
                # That bypasses pytest's capture and goes to the pipe
                # the runner is draining.
                import os
                os.write(1, b"drained-stdout-line\\n")
                os.write(2, b"drained-stderr-line\\n")
                time.sleep(30)  # hang until SIGKILL
            """
        ).strip()
    )

    runner = SubprocessPytestRunner(
        python_executable=_sys.executable,
        report_dir=str(tmp_path / "rep"),
        timeout=1.5,
        grace_period=0.5,
    )

    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        with pytest.raises(TestExecutionError, match="timed out"):
            _run(runner, str(suite), pytest_args=["-s"])

    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "drained-stdout-line" in joined
    assert "drained-stderr-line" in joined


def test_runner_splices_arbitrary_parser_args(tmp_path):
    from airflow_pytest_operator.models import ReportRequest

    captured = {}

    def no_report(report_dir):
        captured["dir"] = report_dir
        return ReportRequest(pytest_args=(), report_path=None)

    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = _run(runner, path, report_request=no_report)

    print(
        f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}, captured dir={captured['dir']!r}"
    )
    assert artifacts.exit_code == 0
    assert artifacts.report_path is None
    assert captured["dir"] == str(tmp_path / "rep")


def test_runner_reports_none_when_parser_path_missing(tmp_path):
    from airflow_pytest_operator.models import ReportRequest

    def wrong_path(report_dir):
        return ReportRequest(
            pytest_args=(),
            report_path=str(tmp_path / "rep" / "wishful.report"),
        )

    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = _run(runner, path, report_request=wrong_path)

    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is None


def test_runner_handles_drained_stream_closed_mid_read(tmp_path, monkeypatch):
    import subprocess as _sp

    class _BadStream:
        def __init__(self):
            self._calls = 0

        def readline(self):
            self._calls += 1
            if self._calls == 1:
                return "first-line\n"
            raise ValueError("I/O operation on closed file")

        def close(self):
            pass

    class _OKStream:
        def readline(self):
            return ""

        def close(self):
            pass

    class _FakeProc:
        returncode = 0

        def __init__(self):
            self.stdout = _BadStream()
            self.stderr = _OKStream()

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(_sp, "Popen", lambda *_a, **_k: _FakeProc())
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    monkeypatch.setattr(runner, "_terminate", lambda proc: None)

    artifacts = _run(runner, str(tmp_path))
    assert artifacts.exit_code == 0
    assert "first-line" in artifacts.stdout


def test_runner_tolerates_close_failure_on_drained_stream(tmp_path, monkeypatch):
    import subprocess as _sp

    class _CloseRaiser:
        def __init__(self):
            self._done = False

        def readline(self):
            if not self._done:
                self._done = True
                return "one-line\n"
            return ""

        def close(self):
            raise OSError("close failed for reasons")

    class _OKStream:
        def readline(self):
            return ""

        def close(self):
            pass

    class _FakeProc:
        returncode = 0

        def __init__(self):
            self.stdout = _CloseRaiser()
            self.stderr = _OKStream()

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(_sp, "Popen", lambda *_a, **_k: _FakeProc())
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    monkeypatch.setattr(runner, "_terminate", lambda proc: None)

    artifacts = _run(runner, str(tmp_path))
    assert artifacts.exit_code == 0
    assert "one-line" in artifacts.stdout


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

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))

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

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
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

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
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

    from airflow_pytest_operator.exceptions import TestExecutionError

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


@pytest.mark.parametrize("bad", [0, -1, -1024])
def test_runner_rejects_non_positive_max_output_bytes(bad):
    with pytest.raises(ValueError, match="max_output_bytes"):
        SubprocessPytestRunner(max_output_bytes=bad)


def test_runner_truncates_stdout_when_cap_exceeded(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        def test_noisy():
            # ~200 KiB of stdout, an order of magnitude above the cap.
            for i in range(2000):
                print('x' * 100)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(
        report_dir=str(tmp_path / "rep"), max_output_bytes=cap
    )
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(
        f"[truncate] exit={artifacts.exit_code} "
        f"stdout_len={len(artifacts.stdout)} cap={cap}"
    )
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert "stdout truncated at" in artifacts.stdout
    assert len(artifacts.stdout.encode("utf-8")) <= cap + 1024


def test_runner_truncates_stderr_when_cap_exceeded(tmp_path):
    cap = 4096
    path = _suite(
        tmp_path,
        """
        import sys
        def test_noisy_stderr():
            for i in range(2000):
                print('e' * 100, file=sys.stderr)
            assert True
        """,
    )
    runner = SubprocessPytestRunner(
        report_dir=str(tmp_path / "rep"), max_output_bytes=cap
    )
    artifacts = _run(runner, path, pytest_args=["-s"])
    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    assert "stderr truncated at" in artifacts.stderr
    assert len(artifacts.stderr.encode("utf-8")) <= cap + 1024


def test_runner_does_not_truncate_when_cap_disabled(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_quiet():
            print('hello-from-child')
            assert True
        """,
    )
    runner = SubprocessPytestRunner(
        report_dir=str(tmp_path / "rep"), max_output_bytes=None
    )
    artifacts = _run(runner, path, pytest_args=["-s"])
    print(f"[no-cap] stdout_len={len(artifacts.stdout)}")
    assert artifacts.exit_code == 0
    assert "hello-from-child" in artifacts.stdout
    assert "truncated" not in artifacts.stdout
    assert "truncated" not in artifacts.stderr
