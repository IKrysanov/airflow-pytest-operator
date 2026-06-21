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


"""Process lifecycle: capture, timeout, cancel, reuse, parallel instances,
terminate. Shared fakes in _run_helpers."""

from __future__ import annotations

import os
import sys
import textwrap

import pytest
from _run_helpers import (
    _run,
    _suite,
)

from airflow_pytest_operator.exceptions import TestExecutionError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.runners import SubprocessPytestRunner


def test_run_times_out_raises_execution_error(tmp_path):
    path = _suite(tmp_path, "import time\ndef test_slow(): time.sleep(60)\n")
    runner = SubprocessPytestRunner(timeout=1, grace_period=2.0)
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
        SubprocessPytestRunner(),
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
        SubprocessPytestRunner(),
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
    report_request = JUnitResultParser(report_dir=str(rep)).report_request
    artifacts = SubprocessPytestRunner().run(path, report_request=report_request)

    print(artifacts.working_dir)

    assert artifacts.working_dir == str(rep)


def test_resolve_cwd_anchors_node_id_selectors_on_path_portion(tmp_path):
    runner = SubprocessPytestRunner()
    suite_file = tmp_path / "x.py"
    suite_file.write_text("def test_a(): pass\n")

    # A node-id selector is anchored on its path portion (before "::"): the
    # file exists, so we derive its parent -- exactly as the bare file would.
    # This is what keeps relative addopts working on a failed_only retry.
    assert runner._resolve_cwd([str(suite_file) + "::test_a"]) == str(tmp_path)
    # Class chains / parametrize ids (extra "::" / "[...]") don't change the
    # anchor -- only the path before the FIRST "::" matters.
    assert runner._resolve_cwd([str(suite_file) + "::TestC::test_a[b::c]"]) == str(
        tmp_path
    )
    # A node-id whose path portion doesn't exist -> None (nothing to anchor on).
    assert runner._resolve_cwd([str(tmp_path / "missing.py") + "::test_a"]) is None
    # A bare "::name" with no path portion -> None (inherited cwd).
    assert runner._resolve_cwd(["::orphan_name"]) is None

    # A plain file plus a node-id selector for the SAME file collapses to the
    # one shared parent (both resolve to tmp_path).
    assert runner._resolve_cwd([str(suite_file), str(suite_file) + "::test_a"]) == str(
        tmp_path
    )

    # Non-selector paths that don't exist on disk: None.
    assert runner._resolve_cwd([str(tmp_path / "tests" / "*.py")]) is None
    assert runner._resolve_cwd([]) is None

    # Sanity: plain paths still get the "deduce from file/dir" treatment.
    assert runner._resolve_cwd([str(suite_file)]) == str(tmp_path)
    assert runner._resolve_cwd([str(tmp_path)]) == str(tmp_path)


def test_resolve_target_paths_absolutises_node_id_path_portion(tmp_path):
    # When the cwd is derived, a node-id selector's path portion is made
    # absolute while the ::test suffix is preserved verbatim -- so pytest,
    # running from the derived cwd, neither double-joins nor loses the selector.
    runner = SubprocessPytestRunner()
    out = runner._resolve_target_paths(
        ["tests/x.py::TestC::test_a[1::2]"], str(tmp_path)
    )
    assert out == [os.path.abspath("tests/x.py") + "::TestC::test_a[1::2]"]

    # Explicit cwd -> verbatim, selector and all.
    explicit = SubprocessPytestRunner(cwd=str(tmp_path))
    assert explicit._resolve_target_paths(["tests/x.py::test_a"], str(tmp_path)) == [
        "tests/x.py::test_a"
    ]


def test_resolve_cwd_uses_commonpath_for_multiple_paths(tmp_path):
    """Multiple targets -> cwd is the closest shared parent.

    The whole point: ``addopts = --alluredir=allure-results`` should
    drop artefacts at the common root of the chosen suites (typically
    ``tests/``), not inside the first suite's subfolder.
    """
    runner = SubprocessPytestRunner()
    tests_root = tmp_path / "tests"
    a_dir = tests_root / "a"
    b_dir = tests_root / "b"
    a_dir.mkdir(parents=True)
    b_dir.mkdir()
    file_a = a_dir / "test_a.py"
    file_b = b_dir / "test_b.py"
    file_a.write_text("def test_one(): pass\n")
    file_b.write_text("def test_two(): pass\n")

    # Two files under tests/{a,b}/ -> tests/ is the common parent.
    cwd = runner._resolve_cwd([str(file_a), str(file_b)])
    assert cwd == str(tests_root)

    # File + sibling directory: still tests/.
    cwd = runner._resolve_cwd([str(file_a), str(b_dir)])
    assert cwd == str(tests_root)

    # Both pointing at the same dir collapses to that dir.
    cwd = runner._resolve_cwd([str(a_dir), str(a_dir)])
    assert cwd == str(a_dir)

    # If a non-selector entry doesn't exist, we bail to None for the
    # whole list -- can't safely chdir for an entry we can't resolve.
    cwd = runner._resolve_cwd([str(file_a), str(tmp_path / "ghost.py")])
    assert cwd is None


def test_stale_cancel_does_not_abort_next_run(tmp_path):
    path = _suite(tmp_path, "def test_a(): assert True")
    runner = SubprocessPytestRunner()
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

    runner = SubprocessPytestRunner()
    dead = _sp.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    assert dead.poll() is not None
    runner._terminate(dead)


def test_terminate_handles_process_lookup_on_sigterm(tmp_path, monkeypatch):
    runner = SubprocessPytestRunner()

    class _FakeProc:
        returncode = None
        pid = 4321

        def poll(self):
            return None

    def _raise_lookup(proc, sig):
        raise ProcessLookupError

    monkeypatch.setattr(runner, "_signal_group", _raise_lookup)
    runner._terminate(_FakeProc())  # type: ignore[arg-type]


def test_terminate_handles_process_lookup_on_sigkill(tmp_path, monkeypatch):
    import signal as _signal_module
    import subprocess as _sp

    runner = SubprocessPytestRunner(grace_period=0.1)

    class _FakeProc:
        returncode = None
        pid = 4322

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
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path, report_request=no_report)

    print(
        f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}, captured dir={captured['dir']!r}"
    )
    assert artifacts.exit_code == 0
    assert artifacts.report_path is None
    # The runner offered the parser a temp fallback directory.
    assert os.path.basename(captured["dir"]).startswith("pytest_report_")


def test_runner_reports_none_when_parser_path_missing(tmp_path):
    from airflow_pytest_operator.models import ReportRequest

    def wrong_path(report_dir):
        return ReportRequest(
            pytest_args=(),
            report_path=str(tmp_path / "rep" / "wishful.report"),
        )

    path = _suite(tmp_path, "def test_ok(): assert True")
    runner = SubprocessPytestRunner()
    artifacts = _run(runner, path, report_request=wrong_path)

    print(f"exit_code={artifacts.exit_code}, report_path={artifacts.report_path!r}")
    assert artifacts.exit_code == 0
    assert artifacts.report_path is None


def test_runner_handles_drained_stream_closed_mid_read(tmp_path, monkeypatch, caplog):
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
    runner = SubprocessPytestRunner()
    monkeypatch.setattr(runner, "_terminate", lambda proc: None)

    with caplog.at_level(
        "WARNING", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, str(tmp_path))
    assert artifacts.exit_code == 0
    assert "first-line" in artifacts.stdout
    # The failure happened on the READ side -> the log must say "draining",
    # not "closing" (which would be the close() path in finally). Regression
    # for the copy-pasted "close stream after drain" message that fired for
    # both cases and made the two indistinguishable.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("error draining pytest output stream" in m for m in msgs), msgs
    assert not any("error closing" in m for m in msgs), msgs


def test_runner_tolerates_close_failure_on_drained_stream(
    tmp_path, monkeypatch, caplog
):
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
    runner = SubprocessPytestRunner()
    monkeypatch.setattr(runner, "_terminate", lambda proc: None)

    with caplog.at_level(
        "WARNING", logger="airflow_pytest_operator.runners.subprocess_runner"
    ):
        artifacts = _run(runner, str(tmp_path))
    assert artifacts.exit_code == 0
    assert "one-line" in artifacts.stdout
    # The read loop succeeded; only close() failed -> the log must point at
    # the close path ("closing ... after drain"), not the read path.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("error closing pytest output stream after drain" in m for m in msgs), (
        msgs
    )
    assert not any("error draining" in m for m in msgs), msgs
