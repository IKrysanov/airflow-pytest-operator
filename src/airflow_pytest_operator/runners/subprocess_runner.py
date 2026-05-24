"""Subprocess-based pytest runner.

Runs ``{python} -m pytest`` as a child process using the *same*
interpreter and virtualenv as the Airflow worker (``sys.executable``).
This gives us the user's requested "same environment" semantics while
keeping pytest's global-state mutations out of the worker process.

Why a child process and not ``pytest.main()`` in-process:
  * pytest mutates sys.modules, import caches, logging, and cwd;
  * Airflow itself may import pytest internals;
  * a crashing/segfaulting test would take the worker down.
The child process is throwaway, so none of that leaks.

Cancellation
------------
The child is started in its own process *group* (POSIX) or *job-like*
session (Windows) so that ``cancel`` can terminate the entire tree --
pytest spawns its own children (xdist workers, subprocesses inside
tests), and signalling only the direct child would orphan them. The
cancel path is graceful by default: SIGTERM, wait ``grace_period``
seconds, then SIGKILL.
"""

# Copyright 2026 Ilya Krysanov
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
    """Run pytest via ``python -m pytest`` in a child process."""

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
        # and is never removed. ``_owns_report_dir`` records that ownership
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
        os.makedirs(report_dir, exist_ok=True)
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
        if _IS_WINDOWS:
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
        except FileNotFoundError as exc:
            raise TestExecutionError(
                f"Could not launch pytest with interpreter {self._python!r}: {exc}"
            ) from exc

        with self._lock:
            # If cancel() landed before the process was registered, honour
            # it immediately rather than letting an orphan run to completion.
            self._proc = proc
            if self._cancelled:
                self._terminate_locked(proc)

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
        """
        with self._lock:
            self._cancelled = True
            proc = self._proc
            if proc is None or proc.poll() is not None:
                return
            self._terminate_locked(proc)

    # -- internals -------------------------------------------------------

    def _terminate(self, proc: subprocess.Popen[str]) -> None:
        with self._lock:
            self._terminate_locked(proc)

    def _terminate_locked(self, proc: subprocess.Popen[str]) -> None:
        """Graceful group termination: SIGTERM -> grace -> SIGKILL.

        Must be called while holding ``self._lock``.
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
        if _IS_WINDOWS:
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
