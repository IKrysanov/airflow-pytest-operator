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
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
from collections.abc import Sequence
from typing import Any

from ..exceptions import TestExecutionError
from ..models import RunArtifacts
from .base import PytestRunner

_IS_WINDOWS = os.name == "nt"


class SubprocessPytestRunner(PytestRunner):
    """Run pytest as a child process via ``{python} -m pytest``.

    This is the default :class:`PytestRunner`. It launches pytest in a
    separate process using, by default, the Airflow worker's own
    interpreter and virtualenv (``sys.executable``). Running in a child
    process — rather than calling ``pytest.main()`` in-process — keeps
    pytest's global-state mutations (``sys.modules``, plugin registration,
    logging, cwd) out of the long-lived worker, and ensures a crashing or
    segfaulting test cannot bring the worker down. The child is always
    started in its own process group/session so the entire tree (including
    ``xdist`` workers and subprocesses spawned by tests) can be terminated
    together on cancel or timeout.

    The runner only ever adds ``--junitxml`` (and ``-o junit_logging=all``)
    to the pytest invocation; all other configuration — plugins such as
    Allure, ``addopts``, markers — is discovered by pytest itself from the
    test tree's ``pytest.ini`` / ``pyproject.toml`` as usual.

    Concurrency model
    -----------------
    A single instance is stateful per run: it tracks one child process and
    at most one auto-created temporary directory on ``self``. It therefore
    supports exactly **one active run at a time** and rejects a concurrent
    ``run()`` on the same instance fail-fast with
    :class:`~airflow_pytest_operator.exceptions.TestExecutionError`. The
    same instance *can* be reused for sequential runs (e.g. an Airflow task
    retry). Separate instances are fully independent and safe to run in
    parallel — the normal Airflow case, where each task gets its own
    operator and thus its own runner. ``run`` and ``cancel`` may be invoked
    from different threads (Airflow calls ``on_kill`` from a signal-driven
    path); shared state is guarded by an internal lock, which is never held
    across the graceful-termination wait.

    :param python_executable: interpreter used to run pytest. Defaults to
        ``sys.executable`` (the worker's own interpreter and virtualenv).
    :param timeout: optional wall-clock limit in seconds for the pytest
        process. On expiry the process tree is terminated and
        :class:`TestExecutionError` is raised; the run is never left
        orphaned.
    :param report_dir: directory for the JUnit report. If given, it is used
        as-is and **never removed** by :meth:`cleanup` (it is treated as
        user-owned data). If omitted, a unique temporary directory is
        created per run and is subject to the ``cleanup`` policy.
    :param cwd: working directory for the pytest process. If omitted, it is
        derived from ``test_path`` (a directory target becomes the cwd, a
        file target's parent becomes the cwd) so that relative paths in
        pytest ``addopts`` — e.g. Allure's ``--alluredir`` — resolve next to
        the tests rather than against the worker's cwd. The absolute JUnit
        path is unaffected.
    :param grace_period: seconds to wait after ``SIGTERM`` before escalating
        to ``SIGKILL`` when terminating the process tree (default 10.0).
    :param cleanup: temporary-directory cleanup policy, one of:
        ``"always"`` (default) — remove the auto-created report dir after
        every run, including on test failure and on task kill;
        ``"on_success"`` — keep it when the run failed (for post-mortem),
        remove it on success; ``"never"`` — never remove it (e.g. when it is
        uploaded as a CI artifact). A user-supplied ``report_dir`` is never
        removed under any policy. Invalid values raise :class:`ValueError`.
    """

    def __init__(
        self,
        *,
        python_executable: str | None = None,
        timeout: int | None = None,
        report_dir: str | None = None,
        cwd: str | None = None,
        grace_period: float = 10.0,
        cleanup: str = "always",
    ) -> None:
        if cleanup not in ("always", "on_success", "never"):
            raise ValueError(
                "cleanup must be one of 'always', 'on_success', 'never'; "
                f"got {cleanup!r}"
            )
        # Default to the worker's own interpreter -> same venv/deps.
        self._python = python_executable or sys.executable
        self._timeout = timeout
        self._report_dir = report_dir
        self._cwd = cwd
        self._grace_period = grace_period
        self._cleanup = cleanup

        # Cleanup bookkeeping. We only ever delete a directory we created
        # ourselves via mkdtemp; a user-supplied report_dir is their data
        # and is never removed. ``_created_report_dir`` records that ownership
        # for the most recent run, so the operator can call cleanup() safely.
        self._created_report_dir: str | None = None

        # Cancellation state. ``run`` and ``cancel`` may be called from
        # different threads (Airflow invokes on_kill from a signal-driven
        # path), so the handle to the live process is guarded by a lock.
        self._proc: subprocess.Popen[str] | None = None
        self._cancelled = False
        self._lock = threading.Lock()

        # Single-run contract. This runner is stateful per run (it tracks
        # one child process and one temp dir on ``self``), so it supports
        # exactly one active run() at a time. Concurrent run() calls on the
        # SAME instance would race on that state; we reject them fail-fast
        # rather than silently leak a temp dir or kill the wrong process.
        # NOTE: separate runner instances are fully independent and safe to
        # run in parallel -- which is the normal Airflow case (one task ->
        # its own operator -> its own runner).
        self._running = False

    def _resolve_cwd(self, test_path: str) -> str | None:
        """Decide the working directory for the pytest child process.

        An explicit ``cwd`` always wins. Otherwise we derive it from the
        test target: a directory becomes its own cwd, a file becomes its
        parent. Running pytest *from its own folder* makes relative paths
        in ``addopts`` (e.g. ``--alluredir=allure-results``) resolve where
        users expect -- next to the tests -- rather than against the
        Airflow worker's cwd. pytest still discovers ``pytest.ini`` and
        ``rootdir`` on its own; this only fixes relative-path resolution.
        """
        if self._cwd is not None:
            return self._cwd
        if not os.path.exists(test_path):
            # Don't guess for node-id style targets ("tests/x.py::test_a")
            # or globs; let pytest handle them from the inherited cwd.
            return None
        if os.path.isdir(test_path):
            return os.path.abspath(test_path)
        return os.path.dirname(os.path.abspath(test_path))

    def run(
        self,
        test_path: str,
        *,
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
    ) -> RunArtifacts:
        # Enforce the single-run contract atomically. A second concurrent
        # run() on the SAME instance would race on the per-run state stored
        # on ``self`` (temp dir, child process), so we reject it fail-fast.
        # The flag is released when run() returns or raises (finally below),
        # so the same instance can be reused for *sequential* runs (e.g. an
        # Airflow task retry). Separate instances are always independent.
        with self._lock:
            if self._running:
                raise TestExecutionError(
                    "This runner is already executing a pytest run. "
                    "SubprocessPytestRunner is single-use per run: create a "
                    "separate instance for concurrent runs (the operator does "
                    "this automatically per task)."
                )
            self._running = True
            self._cancelled = False  # reset stale cancel from a prior run

        try:
            return self._run_locked(test_path, pytest_args=pytest_args, env=env)
        finally:
            with self._lock:
                self._running = False

    def _run_locked(
        self,
        test_path: str,
        *,
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
    ) -> RunArtifacts:
        if self._report_dir is not None:
            report_dir = self._report_dir
            self._created_report_dir = None  # user-owned, never cleaned
        else:
            report_dir = tempfile.mkdtemp(prefix="pytest_report_")
            self._created_report_dir = report_dir  # ours to clean
        try:
            os.makedirs(report_dir, exist_ok=True)
        except OSError as exc:
            raise TestExecutionError(
                f"Could not prepare report directory {report_dir!r}: {exc}"
            ) from exc
        junit_path = os.path.join(report_dir, "junit.xml")

        effective_cwd = self._resolve_cwd(test_path)

        cmd = [
            self._python,
            "-m",
            "pytest",
            test_path,
            f"--junitxml={junit_path}",
            # -o junit_logging makes failure messages land in the XML.
            "-o",
            "junit_logging=all",
        ]
        if pytest_args:
            cmd.extend(pytest_args)

        run_env = os.environ.copy()
        if env:
            run_env.update(env)

        # Detach into a new process group/session so cancel() can reach
        # the whole tree. On POSIX, start_new_session=True calls setsid().
        popen_kwargs: dict[str, Any] = {}
        if _IS_WINDOWS:  # pragma: no cover - Windows-only; CI runs on Linux
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        else:
            popen_kwargs["start_new_session"] = True

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=effective_cwd,
                env=run_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                **popen_kwargs,
            )
        except OSError as exc:
            # OSError covers the full range of process-launch failures
            # (FileNotFoundError = missing interpreter, PermissionError =
            # not executable, NotADirectoryError = bad cwd, etc.), all of
            # which mean "pytest could not be launched". We deliberately do
            # NOT catch broad Exception here: a TypeError/ValueError would be
            # a bug in our own argument handling and should fail loudly with
            # its real traceback, not be masked as a launch failure.
            raise TestExecutionError(
                f"Could not launch pytest with interpreter {self._python!r}: {exc}"
            ) from exc

        with self._lock:
            # If cancel() landed before the process was registered, honour
            # it immediately rather than letting an orphan run to completion.
            # This only triggers when cancel() runs from another thread in the
            # tiny window between run()'s stale-flag reset and this check, so
            # it is a genuine race-guard with no deterministic unit test; it is
            # left uncovered by design rather than asserted via a flaky timing
            # test. (Pre-`run()` cancel is intentionally treated as stale and
            # reset -- see test_stale_cancel_does_not_abort_next_run.)
            self._proc = proc
            cancelled_early = self._cancelled
        if cancelled_early:
            # Terminate outside the lock (graceful wait must not hold it).
            self._terminate(proc)

        try:
            stdout, stderr = proc.communicate(timeout=self._timeout)
        except subprocess.TimeoutExpired as exc:
            # Timeout is an execution failure, but we still must not leave
            # the tree running -- reuse the same group-kill path.
            self._terminate(proc)
            proc.communicate()
            raise TestExecutionError(
                f"pytest run timed out after {self._timeout}s"
            ) from exc
        finally:
            with self._lock:
                self._proc = None

        produced = junit_path if os.path.exists(junit_path) else None
        return RunArtifacts(
            exit_code=proc.returncode,
            junit_xml_path=produced,
            stdout=stdout or "",
            stderr=stderr or "",
            working_dir=report_dir,
        )

    def cleanup(self, *, success: bool = True) -> None:
        """Remove the auto-created report directory, per the cleanup policy.

        Only a directory created by this runner (via ``mkdtemp``) is ever
        removed -- a user-supplied ``report_dir`` is left untouched. The
        operator calls this after the parser has consumed the report, so
        deletion can't race with reading.

        Policy (``cleanup`` ctor arg):
          * ``"always"``     -- remove regardless of outcome (default);
          * ``"on_success"`` -- keep on failure for post-mortem;
          * ``"never"``      -- keep always (e.g. CI artifact upload).
        """
        path = self._created_report_dir
        if path is None:
            return  # nothing of ours to remove
        if self._cleanup == "never":
            return
        if self._cleanup == "on_success" and not success:
            return
        # Guard the read-modify-delete against a concurrent cleanup() from
        # on_kill (different thread). Claim the path under the lock so only
        # one caller performs the rmtree; the other sees None and bails.
        with self._lock:
            path = self._created_report_dir
            if path is None:
                return
            self._created_report_dir = None
        shutil.rmtree(path, ignore_errors=True)

    def cancel(self) -> None:
        """Terminate the running pytest process tree, if any.

        Safe to call when no run is active and safe to call more than
        once -- both are no-ops. Idempotency matters because Airflow may
        deliver more than one termination signal.

        The lock is held only long enough to snapshot the process handle;
        the (potentially multi-second) graceful wait happens WITHOUT the
        lock, so on_kill never serializes run()'s finally or cleanup().
        """
        with self._lock:
            self._cancelled = True
            proc = self._proc
        if proc is None or proc.poll() is not None:
            return
        self._terminate(proc)

    # -- internals -------------------------------------------------------

    def _terminate(self, proc: subprocess.Popen[str]) -> None:
        """Graceful group termination: SIGTERM -> grace -> SIGKILL.

        Must be called WITHOUT holding ``self._lock``: it blocks for up to
        ``grace_period`` seconds waiting for the process to exit, and holding
        the lock across that wait would stall run()'s teardown and cleanup().
        ``proc`` is a snapshot taken by the caller under the lock.
        """
        if proc.poll() is not None:
            return
        try:
            self._signal_group(proc, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=self._grace_period)
            return
        except subprocess.TimeoutExpired:
            pass
        # Still alive after grace period -- hard kill the whole group.
        try:
            self._signal_group(proc, signal.SIGKILL)
        except ProcessLookupError:
            return

    @staticmethod
    def _signal_group(proc: subprocess.Popen[str], sig: int) -> None:
        """Send a signal to the child's entire process group."""
        if _IS_WINDOWS:  # pragma: no cover - Windows-only; CI runs on Linux
            # No POSIX process groups; CREATE_NEW_PROCESS_GROUP lets us
            # send CTRL_BREAK, and as a fallback we kill the child.
            if sig == signal.SIGKILL:
                proc.kill()
            else:
                try:
                    proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                except (ValueError, OSError):
                    proc.terminate()
            return
        # POSIX: negative pid targets the whole group created by setsid().
        os.killpg(os.getpgid(proc.pid), sig)
