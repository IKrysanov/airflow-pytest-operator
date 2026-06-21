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


"""Multi-positional test_path handling. Shared fakes in _run_helpers."""

from __future__ import annotations

import os
import textwrap

import pytest
from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_run_with_list_of_paths_runs_them_all_as_positionals(tmp_path):
    file_a = tmp_path / "a" / "test_a.py"
    file_b = tmp_path / "b" / "test_b.py"
    file_a.parent.mkdir()
    file_b.parent.mkdir()
    file_a.write_text("def test_one(): assert True\n")
    file_b.write_text("def test_two(): assert True\n")

    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = runner.run(
        [str(file_a), str(file_b)],
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(
        f"[multi_paths] exit={artifacts.exit_code} "
        f"total={result.total} cases={[c.node_id for c in result.cases]}"
    )
    assert artifacts.exit_code == 0
    assert result.total == 2
    # Both files contributed exactly one test each.
    node_ids = sorted(c.name for c in result.cases)
    assert node_ids == ["test_one", "test_two"]


def test_run_with_list_of_node_id_selectors_filters_to_specific_tests(tmp_path):
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
    parser = JUnitResultParser()
    # Re-run only test_a and test_c, skip test_b.
    artifacts = runner.run(
        [f"{suite}::test_a", f"{suite}::test_c"],
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    selected = sorted(c.name for c in result.cases)
    print(f"[multi_selectors] selected={selected}")
    assert selected == ["test_a", "test_c"]
    # test_b must NOT have been collected -- it's not in the selector list.
    assert all("test_b" not in name for name in selected)


def test_run_with_string_test_path_unchanged_behaviour(tmp_path):
    suite = tmp_path / "test_x.py"
    suite.write_text("def test_a(): assert True\n")

    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    artifacts = runner.run(
        str(suite),  # str, not list -- exercises the normalisation path
        pytest_args=[],
        report_request=parser.report_request,
    )
    result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    print(f"[string_compat] total={result.total}")
    assert result.total == 1


def test_run_with_empty_list_raises_test_execution_error(tmp_path):
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    with pytest.raises(TestExecutionError, match="test_path must be a non-empty"):
        runner.run(
            [],
            pytest_args=[],
            report_request=parser.report_request,
        )
    print("[empty_list] raised TestExecutionError as expected")


def test_run_with_blank_only_targets_raises(tmp_path):
    # All targets blank (e.g. a Jinja expression that rendered to "") -> after
    # filtering nothing remains, so we fail like the empty-sequence case.
    runner = SubprocessPytestRunner()
    parser = JUnitResultParser()
    for bad in ("", "   ", ["", "  "]):
        with pytest.raises(TestExecutionError, match="non-blank"):
            runner.run(bad, pytest_args=[], report_request=parser.report_request)
    print("[blank_targets] raised TestExecutionError as expected")


def test_run_filters_blank_targets_but_keeps_valid_ones(tmp_path, caplog):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
    with caplog.at_level(
        "WARNING", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, [path, "", "   "])

    assert artifacts.exit_code == 0
    assert artifacts.report_path is not None
    msgs = [r.getMessage() for r in caplog.records]
    assert any("Ignoring 2 empty/blank test target" in m for m in msgs), msgs


def test_run_with_relative_node_id_selector_as_test_path_works(tmp_path):
    suite_dir = tmp_path / "tests"
    suite_dir.mkdir()
    suite_file = suite_dir / "test_x.py"
    suite_file.write_text("def test_y(): pass\ndef test_z(): pass\n")

    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        runner = SubprocessPytestRunner()
        parser = JUnitResultParser()
        artifacts = runner.run(
            "tests/test_x.py::test_y",  # relative selector
            pytest_args=[],
            report_request=parser.report_request,
        )
        result = parser.parse(artifacts.report_path, exit_code=artifacts.exit_code)
    finally:
        os.chdir(orig_cwd)

    print(
        f"[relative_selector] exit={artifacts.exit_code} "
        f"total={result.total} cases={[c.name for c in result.cases]}"
    )
    # Selector matched -> 1 test ran, exit 0, no "file not found".
    assert artifacts.exit_code == 0
    assert result.total == 1
    assert "file or directory not found" not in (artifacts.stderr or "")
    # And pytest selected ONLY test_y, not test_z -- the whole point
    # of passing a specific selector.
    assert [c.name for c in result.cases] == ["test_y"]


def test_early_cancel_returns_artifacts_without_raising(tmp_path, monkeypatch):
    import subprocess as _subprocess

    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(grace_period=2.0)

    real_popen = _subprocess.Popen

    def _popen_then_cancel(*args, **kwargs):
        proc = real_popen(*args, **kwargs)
        # _proc not registered yet -> cancel() only flips the flag, and the
        # post-Popen check in _run_locked terminates the just-launched tree.
        runner.cancel()
        return proc

    monkeypatch.setattr(
        "airflow_pytest_operator.runners.subprocess_runner.subprocess.Popen",
        _popen_then_cancel,
    )

    # Does NOT raise -- a normal RunArtifacts comes back.
    artifacts = _run(runner, path)

    print(
        f"[early_cancel] exit_code={artifacts.exit_code} "
        f"report_path={artifacts.report_path!r}"
    )
    # No report was written (killed before pytest could produce one).
    assert artifacts.report_path is None
    # The exit code reflects termination by signal: on POSIX a process killed
    # by SIGTERM/SIGKILL surfaces as a negative returncode. The 60s sleep never
    # completed normally, so the code is non-zero either way.
    assert artifacts.exit_code != 0
    if os.name != "nt":
        assert artifacts.exit_code < 0, (
            "expected a signal-derived (negative) exit code after termination, "
            f"got {artifacts.exit_code}"
        )
    # Streams are ordinary strings, never None, even on the cancel path.
    assert isinstance(artifacts.stdout, str)
    assert isinstance(artifacts.stderr, str)


def test_report_request_exception_cleans_up_fallback_dir(tmp_path):
    # If the report_request callback raises (a buggy/strict custom parser),
    # the runner has created its fallback temp dir but not yet recorded
    # ownership on self, so a later cleanup() would never reach it. The runner
    # must remove the temp dir before propagating, otherwise every failed run
    # leaks an empty pytest_report_* dir under the system temp.
    path = _suite(tmp_path, "def test_a(): assert True")
    captured = {}

    def boom(report_dir):
        # The fallback dir exists at this point -- record it so we can assert
        # it was cleaned up after the exception unwinds.
        captured["dir"] = report_dir
        assert os.path.isdir(report_dir)
        raise RuntimeError("parser blew up")

    runner = SubprocessPytestRunner()
    with pytest.raises(RuntimeError, match="parser blew up"):
        runner.run(path, report_request=boom)

    print(f"fallback dir was: {captured.get('dir')!r}")
    assert "dir" in captured
    # The temp dir the runner offered must not survive the callback's failure.
    assert not os.path.exists(captured["dir"])
    # The runner claimed no ownership, so a follow-up cleanup() is a safe no-op.
    assert runner._created_report_dir is None
    runner.cleanup(success=False)  # must not raise


def test_timeout_error_carries_captured_streams(tmp_path):
    # On timeout the captured stdout/stderr must be reachable programmatically
    # (via the exception), not only via the worker log -- so an operator/UI can
    # show "why did it hang" without scraping logs.
    import sys as _sys
    import textwrap

    suite = tmp_path / "test_hang.py"
    suite.write_text(
        textwrap.dedent(
            """
            import os, time

            def test_hang():
                # Write straight to fd 1/2 so the bytes bypass pytest capture
                # and reach the pipe the runner drains, then hang until SIGKILL.
                os.write(1, b"hang-stdout-line\\n")
                os.write(2, b"hang-stderr-line\\n")
                time.sleep(30)
            """
        ).strip()
    )
    runner = SubprocessPytestRunner(
        python_executable=_sys.executable, timeout=1.5, grace_period=0.5
    )
    with pytest.raises(TestExecutionError, match="timed out") as excinfo:
        _run(runner, str(suite), pytest_args=["-s"])

    err = excinfo.value
    print(f"stdout attr: {err.stdout!r}\nstderr attr: {err.stderr!r}")
    assert err.stdout is not None and err.stderr is not None
    assert "hang-stdout-line" in err.stdout
    assert "hang-stderr-line" in err.stderr


def test_execution_error_without_output_has_none_streams(tmp_path):
    # A launch failure (missing interpreter) has no associated child output:
    # the stream attributes default to None, and the plain single-arg
    # construction keeps working.
    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner(python_executable="/no/such/python")
    with pytest.raises(TestExecutionError) as excinfo:
        _run(runner, path)
    assert excinfo.value.stdout is None
    assert excinfo.value.stderr is None


def test_invalid_timeout_rejected():
    # Non-positive timeout would make proc.wait() raise immediately, turning
    # every run into an instant timeout -- reject it at construction.
    with pytest.raises(ValueError, match="timeout"):
        SubprocessPytestRunner(timeout=0)
    with pytest.raises(ValueError, match="timeout"):
        SubprocessPytestRunner(timeout=-5)
    # None (no limit) and a positive value are both fine.
    SubprocessPytestRunner(timeout=None)
    SubprocessPytestRunner(timeout=30)


def test_invalid_grace_period_rejected():
    with pytest.raises(ValueError, match="grace_period"):
        SubprocessPytestRunner(grace_period=-1.0)
    # Zero is valid: SIGTERM then escalate to SIGKILL immediately.
    SubprocessPytestRunner(grace_period=0)


def test_run_without_env_overrides_inherits_worker_environment(tmp_path, monkeypatch):
    # With no env overrides the runner passes env=None to Popen so the child
    # inherits the worker's environment directly (no os.environ.copy needed).
    # Prove inheritance works: a var set on the worker is visible to pytest.
    monkeypatch.setenv("INHERITED_FLAG", "from-worker")
    path = _suite(
        tmp_path,
        """
        import os
        def test_inherits():
            assert os.environ.get("INHERITED_FLAG") == "from-worker"
        """,
    )
    artifacts = _run(SubprocessPytestRunner(), path)
    print(f"exit_code={artifacts.exit_code}")
    assert artifacts.exit_code == 0


def test_terminate_falls_back_to_direct_kill_on_killpg_oserror(monkeypatch):
    # _terminate must never let an OSError escape (it runs on the on_kill /
    # timeout paths). If killpg raises something other than ProcessLookupError
    # -- e.g. PermissionError when the child changed gid, or a racing ESRCH --
    # the runner falls back to killing the direct child and returns quietly.
    runner = SubprocessPytestRunner(grace_period=0.1)

    class _FakeProc:
        pid = 4242
        returncode = -9

        def __init__(self):
            self._kill_called = False

        def poll(self):
            return None  # appears alive so _terminate proceeds

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            self._kill_called = True

    proc = _FakeProc()

    def _boom_killpg(*_args, **_kwargs):
        raise PermissionError("operation not permitted")

    # Patch both the group lookup/signal path. getpgid returns a pgid; killpg
    # raises PermissionError -> fallback to proc.kill().
    monkeypatch.setattr(os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(os, "killpg", _boom_killpg)

    # Must not raise, and must have attempted the direct-child kill.
    runner._terminate(proc)  # type: ignore[arg-type]
    print(f"direct kill called: {proc._kill_called}")
    assert proc._kill_called is True


def test_terminate_returns_quietly_when_group_already_gone(monkeypatch):
    # ProcessLookupError from killpg means the whole group already exited:
    # _terminate returns without falling back to a direct kill.
    runner = SubprocessPytestRunner(grace_period=0.1)

    class _FakeProc:
        pid = 5252
        returncode = 0

        def __init__(self):
            self._kill_called = False

        def poll(self):
            return None

        def wait(self, timeout=None):
            return 0

        def kill(self):
            self._kill_called = True

    proc = _FakeProc()

    def _gone_killpg(*_args, **_kwargs):
        raise ProcessLookupError("no such process")

    monkeypatch.setattr(os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(os, "killpg", _gone_killpg)

    runner._terminate(proc)  # type: ignore[arg-type]
    assert proc._kill_called is False


def test_cancel_landing_before_proc_registration_terminates_early(
    tmp_path, monkeypatch
):
    # The race-guard branch in run(): cancel() can set _cancelled in the tiny
    # window after Popen returns but before run() stores the handle in
    # self._proc. At that instant cancel() sees _proc is still None and only
    # flips the flag; the post-Popen check must honour it and terminate the
    # tree, so a just-launched run does not sail past an already-issued cancel.
    # We force the interleaving deterministically (no sleeps/threads) by making
    # Popen call cancel() itself right before handing back the process.
    import subprocess as _subprocess
    import time

    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(grace_period=2.0)

    real_popen = _subprocess.Popen

    def _popen_then_cancel(*args, **kwargs):
        proc = real_popen(*args, **kwargs)
        # _proc is not registered yet -> cancel() just sets the flag.
        runner.cancel()
        return proc

    monkeypatch.setattr(
        "airflow_pytest_operator.runners.subprocess_runner.subprocess.Popen",
        _popen_then_cancel,
    )

    start = time.monotonic()
    artifacts = _run(runner, path)
    elapsed = time.monotonic() - start
    print(f"elapsed={elapsed:.2f}s exit_code={artifacts.exit_code}")
    # The 60s sleep never completes: the early-cancel branch killed the tree.
    assert elapsed < 30, f"early cancel did not terminate the run: {elapsed:.1f}s"
    assert artifacts.report_path is None  # killed before any report was written
