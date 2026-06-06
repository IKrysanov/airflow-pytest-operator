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

import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable, Sequence
from typing import Any

from ..exceptions import TestExecutionError
from ..models import ReportRequest, RunArtifacts
from .base import PytestRunner

_IS_WINDOWS = os.name == "nt"
_log = logging.getLogger(__name__)


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

    The runner is format-agnostic: it does not know whether the report
    is JUnit XML, JSON, or anything else. The parser declares its CLI
    args and report path via the ``report_request`` callback the operator
    passes to :meth:`run`; the runner splices those args into the pytest
    invocation and returns the declared path in :class:`RunArtifacts`.

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
    :param report_dir: directory for the report file requested by the
        parser. If given, it is used as-is and **never removed** by
        :meth:`cleanup` (it is treated as user-owned data). If omitted, a
        unique temporary directory is created per run and is subject to
        the ``cleanup`` policy. The filename inside the directory is
        chosen by the parser, not the runner.
    :param cwd: working directory for the pytest process. If omitted, it is
        derived from ``test_path``: each target is reduced to a directory
        (itself if a directory, its parent if a file) and the closest shared
        parent (``commonpath``) of those becomes the cwd, so that relative
        paths in pytest ``addopts`` — e.g. Allure's ``--alluredir`` — resolve
        next to the tests rather than against the worker's cwd. Node-id
        selectors (``path::test``) disable derivation (cwd falls back to the
        inherited one). The parser-declared report path is unaffected
        (parsers compose it from the absolute ``report_dir``).
    :param grace_period: seconds to wait after ``SIGTERM`` before escalating
        to ``SIGKILL`` when terminating the process tree (default 10.0).
    :param cleanup: temporary-directory cleanup policy, one of:
        ``"always"`` (default) — remove the auto-created report dir after
        every run, including on test failure and on task kill;
        ``"on_success"`` — keep it when the run failed (for post-mortem),
        remove it on success; ``"never"`` — never remove it (e.g. when it is
        uploaded as a CI artifact). A user-supplied ``report_dir`` is never
        removed under any policy. Invalid values raise :class:`ValueError`.
    :param max_output_bytes: per-stream cap on captured ``stdout``/
        ``stderr`` (approximate byte budget; see Implementation note
        below). Default 10 MiB. Once a stream's captured
        size reaches the cap, further chunks from that stream are dropped
        (but the pipe is still drained so the child never blocks on a full
        buffer), and the returned text is suffixed with a one-line marker
        noting the cap. Pass ``None`` to disable the cap. Must be positive.

    Implementation note: the cap is enforced via ``len(chunk)`` --
        i.e. character count, not raw UTF-8 byte count. For ASCII output
        (which is what pytest emits almost entirely: test names, dots/F/E
        markers, file paths) character count equals byte count exactly,
        so the parameter behaves as its name suggests. For pytest runs
        that emit non-ASCII content (e.g. tests asserting on Cyrillic or
        emoji), the actual byte size kept in memory may exceed the cap by
        up to ~4x (UTF-8 multi-byte). The cap still bounds memory in all
        cases; it just bounds it at a slightly different exact value than
        a pure-byte cap would. The parameter is named ``max_output_bytes``
        because (a) for the overwhelming-common pytest-output case the
        two are identical, and (b) bumping precision via per-chunk
        encoding had a measurable cost on long suites.
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
        max_output_bytes: int | None = 10 * 1024 * 1024,
    ) -> None:
        if cleanup not in ("always", "on_success", "never"):
            raise ValueError(
                "cleanup must be one of 'always', 'on_success', 'never'; "
                f"got {cleanup!r}"
            )
        if max_output_bytes is not None and max_output_bytes <= 0:
            raise ValueError(
                "max_output_bytes must be a positive integer or None; "
                f"got {max_output_bytes!r}"
            )
        # Default to the worker's own interpreter -> same venv/deps.
        self._python = python_executable or sys.executable
        self._timeout = timeout
        self._report_dir = report_dir
        self._cwd = cwd
        self._grace_period = grace_period
        self._cleanup = cleanup
        self._max_output_bytes = max_output_bytes

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

    def _resolve_cwd(self, test_paths: Sequence[str]) -> str | None:
        """Decide the working directory for the pytest child process.

        An explicit ``cwd`` always wins. Otherwise we derive it from the
        test targets: each path is reduced to a directory (itself if it
        is one, its parent if it is a file), and we take the
        ``commonpath`` of those directories. Running pytest *from that
        common folder* makes relative paths in ``addopts`` (e.g.
        ``--alluredir=allure-results``) resolve where users expect --
        next to the tests, at the closest shared parent -- rather than
        against the Airflow worker's cwd.

        Node-id selectors (anything containing ``::``) cause us to fall
        back to ``None``: pytest receives those selectors verbatim and
        their path portion is resolved against the inherited cwd, so we
        must not silently chdir under them. pytest still discovers
        ``pytest.ini`` and ``rootdir`` on its own; this only fixes
        relative-path resolution.

        Important: when this method returns a non-``None`` cwd that was
        *derived* (i.e. ``self._cwd`` was not set), the caller MUST pass
        absolute test paths to pytest. The targets here may be relative to
        the worker's cwd; if we chdir into the derived folder but still
        hand pytest the original relative path, pytest resolves it against
        the new cwd and double-joins it (``tests`` -> ``tests/tests``),
        failing with "file or directory not found". See
        :meth:`_resolve_target_paths`.
        """
        if self._cwd is not None:
            return self._cwd
        if not test_paths:
            return None

        dirs: list[str] = []
        for p in test_paths:
            if "::" in p:
                return None
            if not os.path.exists(p):
                return None
            abs_path = os.path.abspath(p)
            if os.path.isdir(abs_path):
                dirs.append(abs_path)
            else:
                dirs.append(os.path.dirname(abs_path))

        if len(dirs) == 1:
            return dirs[0]

        try:
            return os.path.commonpath(dirs)
        except ValueError:
            _log.warning(
                "Cannot derive a common working directory for targets on "
                "different roots (%s); running pytest from the inherited cwd.",
                ", ".join(dirs),
            )
            return None

    def _resolve_target_paths(
        self, test_paths: Sequence[str], effective_cwd: str | None
    ) -> list[str]:
        """Decide the positional test args handed to the pytest child."""

        if effective_cwd is not None and self._cwd is None:
            return [os.path.abspath(p) for p in test_paths]
        return list(test_paths)

    def run(
        self,
        test_path: str | Sequence[str],
        *,
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        report_request: Callable[[str], ReportRequest],
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
            return self._run_locked(
                test_path,
                pytest_args=pytest_args,
                env=env,
                report_request=report_request,
            )
        finally:
            with self._lock:
                self._running = False

    def _run_locked(
        self,
        test_path: str | Sequence[str],
        *,
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        report_request: Callable[[str], ReportRequest],
    ) -> RunArtifacts:
        if isinstance(test_path, str):
            raw_paths: list[str] = [test_path]
        else:
            raw_paths = list(test_path)
        # Drop empty / whitespace-only targets. These are almost always a
        # templating artefact (a Jinja expression that rendered to "") and,
        # if forwarded, would make pytest collect from the cwd or error out.
        # We keep non-blank entries verbatim (paths may legitimately contain
        # spaces) and only discard the blanks.
        test_paths: list[str] = [p for p in raw_paths if p.strip()]
        if len(test_paths) != len(raw_paths):
            _log.warning(
                "Ignoring %d empty/blank test target(s) in test_path.",
                len(raw_paths) - len(test_paths),
            )
        if not test_paths:
            raise TestExecutionError(
                "test_path must be a non-empty string or a non-empty sequence "
                "of non-blank strings. Got no usable target -- if you intended "
                "to use pytest's default discovery, pass an explicit path or set "
                "``testpaths`` in your pytest config."
            )

        if self._report_dir is not None:
            report_dir = self._report_dir
            self._created_report_dir = None  # user-owned, never cleaned
            report_dir_owner = "user-supplied"
        else:
            report_dir = tempfile.mkdtemp(prefix="pytest_report_")
            self._created_report_dir = report_dir  # ours to clean
            report_dir_owner = "auto-created"
        try:
            os.makedirs(report_dir, exist_ok=True)
        except OSError as exc:
            raise TestExecutionError(
                f"Could not prepare report directory {report_dir!r}: {exc}"
            ) from exc

        # Ask the parser what flags to add and where the report will land.
        # The runner never interprets the result -- it splices the args
        # verbatim and reports back whatever path the parser declared.
        spec = report_request(report_dir)

        _log.info(
            "pytest report directory: %s (%s, cleanup=%r); report file: %s",
            os.path.abspath(report_dir),
            report_dir_owner,
            self._cleanup,
            os.path.abspath(spec.report_path)
            if spec.report_path is not None
            else "<none declared by parser>",
        )

        effective_cwd = self._resolve_cwd(test_paths)
        target_paths = self._resolve_target_paths(test_paths, effective_cwd)

        cmd = [
            self._python,
            "-m",
            "pytest",
            *target_paths,
            *spec.pytest_args,
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

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        # Per-stream truncation bookkeeping: [bytes_captured, truncated].
        # Lists are used as cheap mutable boxes shared with the drainer
        # thread; reads/writes happen only inside one drainer per stream,
        # so no extra locking is required.
        stdout_state: list[int] = [0, 0]
        stderr_state: list[int] = [0, 0]
        max_bytes = self._max_output_bytes

        def _drain(stream: Any, sink: list[str], state: list[int]) -> None:
            # readline() is the right primitive here: it returns chunks as
            # they arrive (line-buffered), doesn't block waiting for EOF,
            # and exits cleanly when the pipe closes. read() would buffer
            # the entire stream first, which defeats the purpose for a
            # long-running suite.
            try:
                for chunk in iter(stream.readline, ""):
                    if max_bytes is None or state[0] < max_bytes:
                        sink.append(chunk)
                        state[0] += len(chunk)
                        if max_bytes is not None and state[0] >= max_bytes:
                            state[1] = 1
            except (ValueError, OSError) as e:
                _log.warning("close stream after drain: %s", e)
            finally:
                try:
                    stream.close()
                except Exception as e:  # noqa: BLE001 - best-effort
                    _log.warning("close stream after drain: %s", e)

        # daemon=True so a stuck drainer never blocks interpreter exit; in
        # practice they always exit because the child closes the pipe.
        stdout_thread = threading.Thread(
            target=_drain, args=(proc.stdout, stdout_chunks, stdout_state), daemon=True
        )
        stderr_thread = threading.Thread(
            target=_drain, args=(proc.stderr, stderr_chunks, stderr_state), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()

        timed_out = False
        try:
            proc.wait(timeout=self._timeout)
        except subprocess.TimeoutExpired:
            # Timeout is an execution failure, but we still must not leave
            # the tree running -- reuse the same group-kill path.
            timed_out = True
            self._terminate(proc)
        except BaseException:
            # BaseException (not Exception) on purpose -- AirflowTaskTimeout
            # is an Exception but KeyboardInterrupt is BaseException, and
            # neither should leak a subprocess. We re-raise without altering
            # the type, so Airflow still sees its own AirflowTaskTimeout.
            self._terminate(proc)
            raise
        finally:
            with self._lock:
                self._proc = None

        # Wait for the drainer threads to finish collecting. Once the child
        # is dead, the kernel closes the pipe; readline() then returns "",
        # the iter() sentinel triggers, and the thread exits. We give it a
        # bounded wait so a hung drainer cannot wedge the runner forever
        # -- under that condition we lose the tail but the run still
        # returns rather than hanging.
        stdout_thread.join(timeout=5.0)
        stderr_thread.join(timeout=5.0)

        stdout = "".join(stdout_chunks)
        stderr = "".join(stderr_chunks)
        if stdout_state[1]:
            stdout += (
                f"\n...(stdout truncated at ~{max_bytes} chars; "
                "Update SubprocessPytestRunner.max_output_bytes to capture more)"
            )
        if stderr_state[1]:
            stderr += (
                f"\n...(stderr truncated at ~{max_bytes} chars; "
                "Update SubprocessPytestRunner.max_output_bytes to capture more)"
            )

        if timed_out:
            if stdout:
                _log.warning("pytest stdout captured before timeout:\n%s", stdout)
            if stderr:
                _log.warning("pytest stderr captured before timeout:\n%s", stderr)
            raise TestExecutionError(f"pytest run timed out after {self._timeout}s")

        produced: str | None = None
        if spec.report_path is not None and os.path.exists(spec.report_path):
            produced = spec.report_path

        if produced is not None:
            # The absolute path was already announced before the run (see the
            # "pytest report directory" log above); here we only confirm the
            # outcome, to avoid logging the same path twice.
            _log.info(
                "pytest run finished: exit_code=%d, report written.",
                proc.returncode,
            )
        else:
            # No report file. We log this at DEBUG only: the runner is
            # format-agnostic (it can't tell a missing plugin from a
            # collection crash), and the operator already surfaces this at
            # WARNING with the parser name and captured stderr. Emitting a
            # WARNING here too would just double-log the same failure on the
            # normal operator path. DEBUG keeps the diagnostic available for
            # standalone runner use without the noise.
            _log.debug(
                "pytest run finished: exit_code=%d, no report file at %s.",
                proc.returncode,
                os.path.abspath(spec.report_path)
                if spec.report_path is not None
                else "<none declared by parser>",
            )

        return RunArtifacts(
            exit_code=proc.returncode,
            report_path=produced,
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
          * "always"    -- remove regardless of outcome (default);
          * "on_success" -- keep on failure for post-mortem;
          * "never"     -- keep always (e.g. CI artifact upload).
        """
        path = self._created_report_dir
        if path is None:
            return  # nothing of ours to remove
        if self._cleanup == "never":
            _log.info(
                "Keeping report directory %s (cleanup='never').",
                os.path.abspath(path),
            )
            return
        if self._cleanup == "on_success" and not success:
            _log.info(
                "Keeping report directory %s for post-mortem "
                "(cleanup='on_success', run failed).",
                os.path.abspath(path),
            )
            return
        # Guard the read-modify-delete against a concurrent cleanup() from
        # on_kill (different thread). Claim the path under the lock so only
        # one caller performs the rmtree; the other sees None and bails.
        with self._lock:
            path = self._created_report_dir
            if path is None:
                return
            self._created_report_dir = None
        _log.info("Removing report directory %s (cleanup=%r).", path, self._cleanup)
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
            _log.debug("cancel() called with no live pytest process; nothing to do.")
            return
        _log.warning(
            "Cancellation requested; terminating pytest process tree (pid=%d).",
            proc.pid,
        )
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
        _log.info(
            "Sent SIGTERM to pytest process group (pid=%d); "
            "waiting up to %.1fs for graceful exit.",
            proc.pid,
            self._grace_period,
        )
        try:
            proc.wait(timeout=self._grace_period)
            _log.info("pytest process group exited after SIGTERM (pid=%d).", proc.pid)
            return
        except subprocess.TimeoutExpired:
            pass
        # Still alive after grace period -- hard kill the whole group.
        _log.warning(
            "pytest did not exit within %.1fs of SIGTERM; escalating to SIGKILL "
            "(pid=%d).",
            self._grace_period,
            proc.pid,
        )
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
