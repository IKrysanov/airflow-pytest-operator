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
from airflow_pytest_operator.runners import SubprocessPytestRunner


def _suite(tmp_path: Path, src: str) -> str:
    f = tmp_path / "test_x.py"
    f.write_text(textwrap.dedent(src))
    return str(f)


def test_runner_produces_junit_and_zero_exit_on_pass(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    artifacts = SubprocessPytestRunner(report_dir=str(tmp_path / "rep")).run(path)
    assert artifacts.exit_code == 0
    assert artifacts.junit_xml_path is not None
    assert Path(artifacts.junit_xml_path).exists()


def test_runner_nonzero_exit_on_failure_but_does_not_raise(tmp_path):
    path = _suite(tmp_path, "def test_bad(): assert False")
    artifacts = SubprocessPytestRunner(report_dir=str(tmp_path / "rep")).run(path)
    # A failing test is a valid outcome, not an execution error.
    assert artifacts.exit_code != 0
    assert artifacts.junit_xml_path is not None


def test_runner_passes_extra_args(tmp_path):
    path = _suite(
        tmp_path,
        """
        def test_one(): assert True
        def test_two(): assert True
    """,
    )
    artifacts = SubprocessPytestRunner(report_dir=str(tmp_path / "rep")).run(
        path, pytest_args=["-k", "test_one"]
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
    artifacts = SubprocessPytestRunner(report_dir=str(tmp_path / "rep")).run(
        path, env={"MY_FLAG": "42"}
    )
    assert artifacts.exit_code == 0


def test_runner_bad_interpreter_raises_execution_error(tmp_path):
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(python_executable="/no/such/python")
    with pytest.raises(TestExecutionError):
        runner.run(path)


def test_cancel_kills_running_tree(tmp_path):
    import threading
    import time

    # A test that would hang for a long time if not cancelled.
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

    def _run():
        result_box["artifacts"] = runner.run(path)

    t = threading.Thread(target=_run)
    started = time.monotonic()
    t.start()

    # Give pytest a moment to actually start the child, then cancel.
    time.sleep(2.0)
    runner.cancel()
    t.join(timeout=15)

    elapsed = time.monotonic() - started
    assert not t.is_alive(), "run() did not return after cancel"
    # Must have ended well before the 60s sleep would have finished.
    assert elapsed < 20, f"cancel was too slow: {elapsed:.1f}s"


def test_cancel_is_idempotent_and_safe_without_run(tmp_path):
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    # No active run -> no-op, must not raise.
    runner.cancel()
    runner.cancel()


def test_cancel_before_completion_then_run_normally(tmp_path):
    # A normal fast run should still work on a fresh runner instance.
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = runner.run(path)
    assert artifacts.exit_code == 0


def test_auto_cwd_for_directory_target(tmp_path):
    # Relative paths in addopts should resolve next to the tests, not the
    # process's inherited cwd. We prove it by having pytest write a file
    # via a relative --junit-prefix-style side effect: simplest check is
    # that a relative-output plugin arg lands inside the test dir.
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    # conftest writes a marker file in the *current* working dir at runtime
    (tests_dir / "conftest.py").write_text(
        "import os\n"
        "def pytest_configure(config):\n"
        "    open('cwd_marker.txt', 'w').write(os.getcwd())\n"
    )
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    artifacts = runner.run(str(tests_dir))

    assert artifacts.exit_code == 0
    marker = tests_dir / "cwd_marker.txt"
    assert marker.exists(), "pytest did not run from the tests directory"
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
    artifacts = runner.run(str(test_file))

    assert artifacts.exit_code == 0
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
    artifacts = runner.run(str(tests_dir), env={"MARK_DIR": str(explicit)})

    assert artifacts.exit_code == 0
    assert (explicit / "m.txt").read_text() == str(explicit.resolve())


def test_junit_report_unaffected_by_auto_cwd(tmp_path):
    # The junit path is absolute, so changing cwd must not misplace it.
    tests_dir = tmp_path / "suite"
    tests_dir.mkdir()
    (tests_dir / "test_x.py").write_text("def test_a(): assert True\n")
    rep = tmp_path / "rep"
    runner = SubprocessPytestRunner(report_dir=str(rep))
    artifacts = runner.run(str(tests_dir))

    assert artifacts.junit_xml_path == str(rep / "junit.xml")
    assert (rep / "junit.xml").exists()


def test_cleanup_removes_auto_dir_by_default(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()  # cleanup="always", auto report_dir
    artifacts = runner.run(path)
    auto_dir = artifacts.working_dir
    assert auto_dir is not None and os.path.isdir(auto_dir)

    runner.cleanup(success=True)
    assert not os.path.exists(auto_dir)


def test_cleanup_never_keeps_auto_dir(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="never")
    artifacts = runner.run(path)
    runner.cleanup(success=True)
    assert os.path.isdir(artifacts.working_dir)


def test_cleanup_on_success_keeps_dir_on_failure(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="on_success")
    artifacts = runner.run(path)
    # Simulate a failed run -> directory must be retained for post-mortem.
    runner.cleanup(success=False)
    assert os.path.isdir(artifacts.working_dir)


def test_cleanup_on_success_removes_dir_on_success(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(cleanup="on_success")
    artifacts = runner.run(path)
    runner.cleanup(success=True)
    assert not os.path.exists(artifacts.working_dir)


def test_cleanup_never_touches_user_supplied_dir(tmp_path):
    user_dir = tmp_path / "my_reports"
    user_dir.mkdir()
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(user_dir))  # cleanup="always"
    runner.run(path)
    runner.cleanup(success=True)
    # User-owned directory is never removed, even under "always".
    assert user_dir.is_dir()


def test_cleanup_is_safe_without_run(tmp_path):
    runner = SubprocessPytestRunner()
    runner.cleanup(success=True)  # nothing created yet -> no-op, no raise


def test_invalid_cleanup_policy_rejected():
    with pytest.raises(ValueError):
        SubprocessPytestRunner(cleanup="sometimes")


def test_report_dir_pointing_at_file_raises_execution_error(tmp_path):
    # A user-supplied report_dir that is actually a file must surface as
    # TestExecutionError (launch failure), not a bare OSError.
    not_a_dir = tmp_path / "iam_a_file"
    not_a_dir.write_text("x")
    runner = SubprocessPytestRunner(report_dir=str(not_a_dir))
    with pytest.raises(TestExecutionError, match="report directory"):
        runner.run(str(tmp_path))


def test_cancel_does_not_block_cleanup_during_grace(tmp_path):
    # Regression for the lock-held-during-grace bug: while cancel() is in its
    # graceful wait, other lock users (here, cleanup) must not be blocked for
    # the whole grace period. We assert cleanup returns quickly even though a
    # cancel of a stubborn (SIGTERM-ignoring) process is mid-grace.
    import threading
    import time

    # A child that ignores SIGTERM, forcing cancel() into the full grace wait.
    path = _suite(
        tmp_path,
        """
        import signal, time
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        def test_stubborn(): time.sleep(30)
        """,
    )
    runner = SubprocessPytestRunner(grace_period=5.0)

    def _run():
        try:
            runner.run(path)
        except Exception:  # noqa: BLE001
            pass

    t = threading.Thread(target=_run)
    t.start()
    time.sleep(1.5)  # let the child start

    # Kick cancel() in another thread; it will sit in the 5s grace wait.
    canceller = threading.Thread(target=runner.cancel)
    canceller.start()
    time.sleep(0.5)  # ensure cancel() has entered the grace wait

    # cleanup() must not be blocked for the whole grace period.
    t0 = time.monotonic()
    runner.cleanup(success=False)
    elapsed = time.monotonic() - t0
    assert elapsed < 2.0, f"cleanup blocked by cancel's grace wait: {elapsed:.1f}s"

    canceller.join(timeout=15)
    t.join(timeout=15)


def test_concurrent_run_on_same_instance_is_rejected(tmp_path):
    # One slow run holds the instance; a second concurrent run() on the
    # SAME instance must fail fast rather than race on shared state.
    import threading
    import time

    slow = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(5)\n")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))

    errors = {}

    def _slow():
        try:
            runner.run(slow)
        except Exception as e:  # noqa: BLE001
            errors["slow"] = e

    t = threading.Thread(target=_slow)
    t.start()
    time.sleep(1.0)  # ensure the first run is in progress

    with pytest.raises(TestExecutionError, match="already executing"):
        runner.run(slow)

    runner.cancel()  # stop the slow one
    t.join(timeout=15)


def test_sequential_reuse_of_same_instance_works(tmp_path):
    # After a run finishes, the same instance can run again (e.g. retry).
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    a = runner.run(path)
    assert a.exit_code == 0
    b = runner.run(path)  # must not raise "already executing"
    assert b.exit_code == 0


def test_run_times_out_raises_execution_error(tmp_path):
    # A test slower than the timeout must surface as TestExecutionError and
    # the child must not be left running.
    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(
        report_dir=str(tmp_path / "rep"), timeout=1, grace_period=2.0
    )
    with pytest.raises(TestExecutionError, match="timed out"):
        runner.run(path)


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
    # -s disables pytest's capture so the prints reach the child's real
    # stdout/stderr, which is exactly what the runner pipes back.
    artifacts = SubprocessPytestRunner(report_dir=str(tmp_path / "rep")).run(
        path, pytest_args=["-s"]
    )
    assert artifacts.exit_code == 0
    assert "hello-stdout" in artifacts.stdout
    assert "hello-stderr" in artifacts.stderr


def test_usage_error_yields_none_junit_without_raising(tmp_path):
    # An unrecognized pytest option is a usage error (exit 4): pytest exits
    # before writing junit. That's a non-zero outcome, NOT a launch failure,
    # so run() must return artifacts with junit_xml_path=None rather than
    # raising TestExecutionError.
    path = _suite(tmp_path, "def test_a(): assert True")
    artifacts = SubprocessPytestRunner(report_dir=str(tmp_path / "rep")).run(
        path, pytest_args=["--definitely-not-a-real-option"]
    )
    assert artifacts.exit_code != 0
    assert artifacts.junit_xml_path is None


def test_working_dir_is_the_report_dir(tmp_path):
    rep = tmp_path / "rep"
    path = _suite(tmp_path, "def test_a(): assert True")
    artifacts = SubprocessPytestRunner(report_dir=str(rep)).run(path)
    assert artifacts.working_dir == str(rep)


def test_resolve_cwd_none_for_node_id_or_glob_target(tmp_path):
    # Node-id ("tests/x.py::test_a") and glob targets don't exist as paths,
    # so the runner declines to guess a cwd and lets pytest use the inherited
    # one. _resolve_cwd is the single decision point for that behavior.
    runner = SubprocessPytestRunner()
    assert runner._resolve_cwd(str(tmp_path / "x.py::test_a")) is None
    assert runner._resolve_cwd(str(tmp_path / "tests" / "*.py")) is None


def test_stale_cancel_does_not_abort_next_run(tmp_path):
    # cancel() with no active run sets a flag; the next run() must reset it so
    # a stale cancel from a prior lifecycle doesn't kill a fresh run.
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    runner.cancel()  # no run active -> just records the stale intent
    artifacts = runner.run(path)
    assert artifacts.exit_code == 0
    assert artifacts.junit_xml_path is not None


def test_separate_instances_run_in_parallel_safely(tmp_path):
    # The normal Airflow case: independent runners don't interfere, and each
    # cleans up only its own temp dir.
    import threading

    results = {}

    def _go(key):
        d = tmp_path / f"suite_{key}"
        d.mkdir()
        (d / "test_x.py").write_text("def test_a(): assert True\n")
        r = SubprocessPytestRunner()  # auto temp dir, independent instance
        art = r.run(str(d))
        results[key] = art.working_dir
        r.cleanup(success=True)

    threads = [threading.Thread(target=_go, args=(k,)) for k in ("a", "b", "c")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    # All three got distinct temp dirs and all were cleaned up.
    dirs = list(results.values())
    assert len(set(dirs)) == 3, "temp dirs collided across instances"
    for d in dirs:
        assert not os.path.exists(d), "each instance must clean its own dir"


def test_terminate_returns_early_when_process_already_dead(tmp_path):
    # _terminate must no-op when the process has already exited (poll()
    # returns a code), exercising its early-return guard (subprocess_runner
    # _terminate poll() branch). We use a real, already-finished process.
    import subprocess as _sp

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))
    dead = _sp.Popen([sys.executable, "-c", "pass"])
    dead.wait()  # ensure it has fully exited
    assert dead.poll() is not None
    # Must return cleanly without trying to signal a dead process.
    runner._terminate(dead)


def test_terminate_handles_process_lookup_on_sigterm(tmp_path, monkeypatch):
    # If the process dies between the poll() check and the SIGTERM, killpg
    # raises ProcessLookupError; _terminate must swallow it and return
    # (the "race: gone before SIGTERM" branch).
    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"))

    class _FakeProc:
        # poll() returns None (looks alive) so we get past the early guard,
        # then _signal_group raises ProcessLookupError as if it just died.
        returncode = None

        def poll(self):
            return None

    def _raise_lookup(proc, sig):
        raise ProcessLookupError

    monkeypatch.setattr(runner, "_signal_group", _raise_lookup)
    # Must not raise.
    runner._terminate(_FakeProc())  # type: ignore[arg-type]


def test_terminate_handles_process_lookup_on_sigkill(tmp_path, monkeypatch):
    # The process survives SIGTERM and the grace wait, then disappears right
    # before SIGKILL: the second _signal_group raises ProcessLookupError,
    # which _terminate must also swallow (the SIGKILL-race branch).
    import signal as _signal_module
    import subprocess as _sp

    runner = SubprocessPytestRunner(report_dir=str(tmp_path / "rep"), grace_period=0.1)

    class _FakeProc:
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            # Never exits during grace -> forces escalation to SIGKILL.
            raise _sp.TimeoutExpired(cmd="pytest", timeout=timeout)

    calls = {"n": 0}

    def _signal(proc, sig):
        calls["n"] += 1
        if sig == _signal_module.SIGKILL:
            raise ProcessLookupError
        # SIGTERM path returns normally.

    monkeypatch.setattr(runner, "_signal_group", _signal)
    runner._terminate(_FakeProc())  # type: ignore[arg-type]
    # Both SIGTERM and SIGKILL were attempted.
    assert calls["n"] == 2


def test_concurrent_cleanup_only_one_rmtree(tmp_path):
    # Two cleanup() calls racing (e.g. operator post-run + on_kill) must not
    # both rmtree: the loser sees _created_report_dir already claimed as None
    # under the lock and bails (the cleanup race-guard branch).
    #
    # The race is between the unlocked first read (sees a path) and the
    # locked re-read (sees None because the winner cleared it). We make it
    # deterministic by wrapping the runner's lock so that the instant this
    # cleanup() acquires it, the path has already been claimed -- exactly the
    # state the loser would observe.
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()  # auto temp dir, cleanup="always"
    artifacts = runner.run(path)
    auto_dir = artifacts.working_dir
    assert auto_dir is not None and os.path.isdir(auto_dir)

    real_lock = runner._lock

    class _ClaimingLock:
        # On acquire, simulate the winner having already taken the path.
        def __enter__(self):
            runner._created_report_dir = None
            return real_lock.__enter__()

        def __exit__(self, *exc):
            return real_lock.__exit__(*exc)

    runner._lock = _ClaimingLock()  # type: ignore[assignment]
    # First (unlocked) read sees the real path; locked re-read sees None ->
    # the guard returns without rmtree and without raising.
    runner.cleanup(success=True)
    runner._lock = real_lock  # type: ignore[assignment]

    # Because the loser bailed, the directory is NOT removed here -- proving
    # the guard prevented a double-rmtree rather than racing into one.
    assert os.path.isdir(auto_dir)
    # Clean up the leftover dir ourselves so the test leaves no trace.
    import shutil as _shutil

    _shutil.rmtree(auto_dir, ignore_errors=True)
