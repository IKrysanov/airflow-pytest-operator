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
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable, Sequence
from typing import Any

from ..exceptions import TestExecutionError
from ..models import OutputSink, ReportRequest, RunArtifacts
from .base import PytestRunner

_IS_WINDOWS = os.name == "nt"
_log = logging.getLogger(__name__)

# Values whose KEY name suggests a credential are masked in the verbose runtime
# diagnostics, so a secret never reaches the task log. Substring match,
# case-insensitive. ``AIRFLOW_CONN_*`` is included because its KEY looks
# innocuous while its VALUE is a connection URI that embeds a password
# (e.g. ``postgres://user:pw@host/db``) -- the plain PASSWORD/TOKEN/KEY set
# would miss it.
_SENSITIVE_KEY_RE = re.compile(
    r"PASSWORD|PASSWD|PASSPHRASE|PWD|TOKEN|SECRET|CREDENTIAL|PRIVATE|AUTH|KEY"
    r"|_URI|_URL|_DSN|^AIRFLOW_CONN_",
    re.IGNORECASE,
)


def _mask_env_value(key: str, value: str) -> str:
    """Return ``value`` masked when ``key`` looks like a credential, else as-is."""
    return "***" if _SENSITIVE_KEY_RE.search(key) else value


def _is_within(path: str, directory: str) -> bool:
    """True if ``path`` is ``directory`` itself or lives inside it.

    Used to tell whether a parser placed its report inside the runner's
    fallback temp dir (so the runner owns and cleans it) or somewhere of its
    own (user-owned, never cleaned). Compares *real* paths (``os.path.realpath``
    resolves symlinks): a plain ``abspath`` would treat a symlinked
    ``report_dir`` (e.g. ``/var/tmp`` -> ``/private/var/...``, common on
    macOS) as outside the temp dir even when they are physically the same
    location, which could lead the runner to delete user data through the
    link. realpath collapses both sides to their canonical target first.
    """
    p = os.path.realpath(path)
    d = os.path.realpath(directory)
    return p == d or p.startswith(d + os.sep)


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
    Report location: the runner does NOT own the report directory. It
    prepares a unique temporary directory and offers it to the parser's
    ``report_request`` as a fallback. The *parser* decides where the report
    lands (see :class:`~airflow_pytest_operator.reporters.ResultParser` and
    its ``report_dir`` argument): if the parser declares a path inside the
    runner's temp dir, that dir is owned by the runner and removed per the
    ``cleanup`` policy; if the parser declares its own location, that is
    user-owned data and is never removed (the unused temp dir is discarded
    immediately).

    :param cwd: working directory for the pytest process. If omitted, it is
        derived from ``test_path``: each target is reduced to a directory
        (itself if a directory, its parent if a file) and the closest shared
        parent (``commonpath``) of those becomes the cwd, so that relative
        paths in pytest ``addopts`` — e.g. Allure's ``--alluredir`` — resolve
        next to the tests rather than against the worker's cwd. Node-id
        selectors (``path::test``) are anchored on their path portion, so a
        ``failed_only`` retry (whose targets are all node-ids) derives the same
        cwd a full run would; only a target with no resolvable path on disk
        falls back to the inherited cwd. The parser-declared report path is
        unaffected (parsers compose it from the absolute ``report_dir``).
        Note: when you pass an explicit ``cwd``, a *relative* ``test_path`` is
        forwarded to pytest verbatim and therefore resolves against this
        ``cwd``, not against the worker's cwd — e.g. ``cwd="/proj"`` with
        ``test_path="tests"`` runs ``/proj/tests``. Pass an absolute
        ``test_path`` if you need it resolved elsewhere. (Derived cwds, by
        contrast, absolutise the targets first; see
        :meth:`_resolve_target_paths`.)
    :param grace_period: seconds to wait after ``SIGTERM`` before escalating
        to ``SIGKILL`` when terminating the process tree (default 10.0).
    :param cleanup: temporary-directory cleanup policy, one of:
        ``"always"`` (default) — remove the auto-created report dir after
        every run, including on test failure and on task kill;
        ``"on_success"`` — keep it when the run failed (for post-mortem),
        remove it on success; ``"never"`` — never remove it (e.g. when it is
        uploaded as a CI artifact). A parser-supplied report directory (one
        outside the runner's temp dir) is never removed under any policy.
        Invalid values raise :class:`ValueError`.
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
    :param verbose: when True, log a one-time block of runtime diagnostics
        right before launching pytest -- the fully-resolved command, the
        effective working directory, the env delta against ``os.environ``
        (added vs overridden keys, with credential-looking values masked), and
        the report directory + cleanup policy. Useful for debugging "why did it
        run like that" on a real worker. Default False (no extra logging).

    ``.env`` loading is a per-run input, not runner config: pass ``env_file`` /
    ``env_file_overrides`` to :meth:`run` (the operator forwards them from its
    own parameters), alongside ``env``.
    """

    _DRAIN_JOIN_TIMEOUT: float = 5.0

    def __init__(
        self,
        *,
        python_executable: str | None = None,
        timeout: int | None = None,
        cwd: str | None = None,
        grace_period: float = 10.0,
        cleanup: str = "always",
        max_output_bytes: int | None = 10 * 1024 * 1024,
        verbose: bool = False,
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
        # A non-positive timeout is almost certainly a mistake: 0 (or negative)
        # makes proc.wait() raise TimeoutExpired immediately, so pytest would be
        # killed before it could do anything. Reject it rather than silently
        # turning every run into an instant timeout. ``None`` = no limit.
        if timeout is not None and timeout <= 0:
            raise ValueError(
                f"timeout must be a positive number of seconds or None; got {timeout!r}"
            )
        # A negative grace period is meaningless (you cannot wait a negative
        # time before escalating to SIGKILL). 0 is allowed and means "send
        # SIGTERM, then escalate to SIGKILL immediately".
        if grace_period < 0:
            raise ValueError(
                f"grace_period must be a non-negative number of seconds; "
                f"got {grace_period!r}"
            )
        # Default to the worker's own interpreter -> same venv/deps.
        self._python = python_executable or sys.executable
        self._timeout = timeout
        self._cwd = cwd
        self._grace_period = grace_period
        self._cleanup = cleanup
        self._max_output_bytes = max_output_bytes
        self._verbose = verbose

        # Cleanup bookkeeping. We only ever delete the temp directory we
        # created ourselves via mkdtemp AND that the parser actually used; a
        # parser-supplied report directory is their data and is never removed.
        # ``_created_report_dir`` records that ownership for the most recent
        # run, so the operator can call cleanup() safely.
        self._created_report_dir: str | None = None
        # A parser-supplied (user-owned) report directory for the most recent
        # run. Never removed -- tracked only so cleanup() can log where the
        # report was left, for parity with the owned-temp case.
        self._kept_report_dir: str | None = None

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

    @staticmethod
    def _target_path_part(target: str) -> str:
        """The filesystem-path portion of a target, dropping any ``::`` selector.

        ``"tests/test_x.py::TestC::test_a[b::c]"`` -> ``"tests/test_x.py"``.
        A node-id's path portion is everything before the FIRST ``::`` (later
        ``::`` belong to class chains or parametrize ids, never the path). A
        plain path with no ``::`` is returned unchanged.
        """
        return target.partition("::")[0]

    def _resolve_cwd(self, test_paths: Sequence[str]) -> str | None:
        """Decide the working directory for the pytest child process.

        An explicit ``cwd`` always wins. Otherwise we derive it from the
        test targets: each target is reduced to a directory (itself if it
        is one, its parent if it is a file), and we take the
        ``commonpath`` of those directories. Running pytest *from that
        common folder* makes relative paths in ``addopts`` (e.g.
        ``--alluredir=allure-results``) resolve where users expect --
        next to the tests, at the closest shared parent -- rather than
        against the Airflow worker's cwd.

        Node-id selectors (``path::test``) are anchored on their **path
        portion**: ``tests/test_x.py::test_a`` derives ``tests/`` exactly as the
        bare file would. This matters for the ``failed_only`` retry, whose
        targets are all node-ids -- without it the retry would run from the
        inherited cwd and relative ``addopts`` (Allure's ``--alluredir`` etc.)
        would break on every retry. We can do this safely because
        :meth:`_resolve_target_paths` absolutises the path portion in lock-step,
        so pytest never double-joins. A target whose path portion does not exist
        on disk (or a bare ``::test`` with no path) still falls back to ``None``
        (inherited cwd), since there is nothing to anchor on.

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
            # Anchor on the path portion of a node-id selector (everything
            # before "::"), not the whole string. A bare "::test" has no path
            # to anchor on -> keep the inherited cwd.
            path_part = self._target_path_part(p)
            if not path_part or not os.path.exists(path_part):
                return None
            abs_path = os.path.abspath(path_part)
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
        """Decide the positional test args handed to the pytest child.

        Targets are absolutised **only** when the cwd was *derived* by the
        runner (``self._cwd is None`` and ``_resolve_cwd`` returned a folder).
        That is the one case where leaving them relative would break: the
        derived cwd differs from the worker cwd the relative path was written
        against, so pytest would double-join it (``tests`` -> ``tests/tests``).
        For a node-id selector only the **path portion** is absolutised; the
        ``::test`` selector suffix is preserved verbatim
        (``tests/test_x.py::test_a`` -> ``/abs/tests/test_x.py::test_a``), so a
        derived cwd works for the ``failed_only`` retry too. See
        :meth:`_resolve_cwd`.

        When the caller passed an **explicit** ``cwd``, targets are forwarded
        *verbatim* -- they are NOT absolutised. A relative ``test_path`` then
        resolves against that explicit ``cwd`` (pytest's own cwd), not against
        the worker cwd. This is intentional: with an explicit ``cwd`` the user
        owns both the directory and the targets, so e.g.
        ``SubprocessPytestRunner(cwd="/proj")`` + ``test_path="tests"`` runs
        ``/proj/tests``. If you want a path resolved against the worker cwd
        instead, pass it as an absolute path. (When no cwd is in play at all --
        a bare ``::test``, globs, or non-existent paths -- targets are likewise
        forwarded verbatim and resolved against the inherited cwd.)
        """

        if effective_cwd is not None and self._cwd is None:
            return [self._absolutise_target(p) for p in test_paths]
        return list(test_paths)

    @classmethod
    def _absolutise_target(cls, target: str) -> str:
        """Absolutise a target's path portion, keeping any ``::`` selector.

        ``"tests/test_x.py::test_a"`` -> ``"/abs/tests/test_x.py::test_a"``;
        a plain path is absolutised whole. Only used when the runner derived
        the cwd (see :meth:`_resolve_target_paths`).
        """
        path_part, sep, selector = target.partition("::")
        return os.path.abspath(path_part) + sep + selector

    def _resolve_run_env(
        self,
        env: dict[str, str] | None,
        env_file: str | None,
        env_file_overrides: bool,
    ) -> dict[str, str] | None:
        """Build the child-process environment, or ``None`` to inherit as-is.

        Precedence, lowest to highest: ``os.environ`` -> ``env_file`` -> ``env``.
        The ``env_file`` is parsed with ``dotenv_values`` (a read-only parse --
        the parent ``os.environ`` is never mutated). Keys starting with
        ``AIRFLOW`` are dropped from the file unless ``env_file_overrides`` is
        True, so a stray ``.env`` cannot clobber the worker's Airflow wiring in
        the child. With neither an ``env_file`` nor an ``env``, returns ``None``
        so Popen inherits ``os.environ`` directly -- skipping a full copy on the
        common path.
        """
        # A blank/whitespace ``env_file`` is almost always a templating artefact
        # (a Jinja expression that rendered to "" or "  "); treat it as unset,
        # mirroring how blank test targets / pytest args are dropped, rather than
        # raising "file not found" on it.
        if env_file is not None:
            env_file = env_file.strip() or None
        if not env_file and not env:
            return None
        run_env = os.environ.copy()
        if env_file:
            for key, value in self._read_env_file(env_file).items():
                if value is None:
                    continue  # a bare ``KEY`` with no ``=`` carries no value
                if key.startswith("AIRFLOW") and not env_file_overrides:
                    continue  # never let a .env override the worker's Airflow env
                run_env[key] = value
        if env:
            run_env.update(env)  # explicit env wins, unconditionally
        return run_env

    @staticmethod
    def _read_env_file(env_file: str) -> dict[str, str | None]:
        """Parse ``env_file`` into a dict without touching ``os.environ``.

        Uses ``dotenv_values`` (a read-only parse, unlike ``load_dotenv``).
        Fails fast with a clear :class:`TestExecutionError` when the file is
        missing or when ``python-dotenv`` is not installed -- both are
        actionable configuration problems, not silent fallbacks.
        """
        path = os.path.abspath(env_file)
        if not os.path.isfile(path):
            raise TestExecutionError(
                f"env_file not found: {path!r} (from env_file={env_file!r}). "
                "Pass an absolute path, or one resolvable from the worker's "
                "working directory."
            )
        try:
            from dotenv import dotenv_values
        except ImportError as exc:
            raise TestExecutionError(
                "env_file requires python-dotenv, which is not installed. "
                "Install it with: pip install 'airflow-pytest-operator[dotenv]'"
            ) from exc
        return dict(dotenv_values(path))

    def run(
        self,
        test_path: str | Sequence[str],
        *,
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        env_file: str | None = None,
        env_file_overrides: bool = False,
        report_request: Callable[[str], ReportRequest],
        on_output: OutputSink | None = None,
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
                env_file=env_file,
                env_file_overrides=env_file_overrides,
                report_request=report_request,
                on_output=on_output,
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
        env_file: str | None = None,
        env_file_overrides: bool = False,
        report_request: Callable[[str], ReportRequest],
        on_output: OutputSink | None = None,
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

        # Resolve the child-process environment up front -- a bad env_file fails
        # here, before any temp directory is created, so nothing is leaked.
        run_env = self._resolve_run_env(env, env_file, env_file_overrides)

        # The parser owns the report location. We create a unique temp dir and
        # offer it as a *fallback*: the parser uses it only if it was not given
        # a report_dir of its own. We then detect which happened (by whether
        # the declared report path lives inside our temp dir) to decide
        # ownership for cleanup.
        fallback_dir = tempfile.mkdtemp(prefix="pytest_report_")

        # Ask the parser what flags to add and where the report will land.
        # The runner never interprets the result -- it splices the args
        # verbatim and reports back whatever path the parser declared.
        try:
            spec = report_request(fallback_dir)
        except BaseException:
            shutil.rmtree(fallback_dir, ignore_errors=True)
            raise

        if spec.report_path is not None and _is_within(spec.report_path, fallback_dir):
            # Parser used our fallback -> the temp dir is ours to clean.
            report_dir: str | None = fallback_dir
            self._created_report_dir = fallback_dir
            self._kept_report_dir = None
            report_dir_owner = "auto-created (temp)"
        else:
            # Parser declared its own location (or no report file at all); our
            # temp dir is unused, so discard it now. A parser-supplied dir is
            # user-owned and never cleaned.
            shutil.rmtree(fallback_dir, ignore_errors=True)
            self._created_report_dir = None
            report_dir = (
                os.path.dirname(os.path.abspath(spec.report_path))
                if spec.report_path is not None
                else None
            )
            # Record the user-owned location so cleanup() can still report
            # where the report was left (it is never removed).
            self._kept_report_dir = report_dir
            report_dir_owner = "parser-supplied"

        # Ensure the target directory exists. mkdtemp already created the temp;
        # a parser-supplied directory may not exist yet.
        if report_dir is not None:
            try:
                os.makedirs(report_dir, exist_ok=True)
            except OSError as exc:
                raise TestExecutionError(
                    f"Could not prepare report directory {report_dir!r}: {exc}"
                ) from exc

        _log.info(
            "pytest report directory: %s (%s, cleanup=%r); report file: %s",
            os.path.abspath(report_dir) if report_dir is not None else "<none>",
            report_dir_owner,
            self._cleanup,
            os.path.abspath(spec.report_path)
            if spec.report_path is not None
            else "<none declared by parser>",
        )

        effective_cwd = self._resolve_cwd(test_paths)
        target_paths = self._resolve_target_paths(test_paths, effective_cwd)

        # When streaming, run the interpreter unbuffered (-u) so the child
        # flushes stdout/stderr per write instead of block-buffering them on a
        # pipe -- otherwise readline() (and the live emit below) would not see a
        # line until the child's buffer fills or it exits. No effect on the
        # captured output, and skipped entirely when not streaming.
        interpreter_flags = ["-u"] if on_output is not None else []
        cmd = [
            self._python,
            *interpreter_flags,
            "-m",
            "pytest",
            *target_paths,
            *spec.pytest_args,
        ]
        # Drop empty / whitespace-only user args (typically a Jinja expression
        # that rendered to ""). Unlike test_path, an *empty* pytest_args list is
        # perfectly valid -- it just means "no extra flags" -- so we only filter
        # the blanks and never reject the list itself. The parser's own
        # spec.pytest_args are trusted and spliced verbatim.
        if pytest_args:
            clean_args = [a for a in pytest_args if a.strip()]
            if len(clean_args) != len(pytest_args):
                _log.warning(
                    "Ignoring %d empty/blank pytest arg(s).",
                    len(pytest_args) - len(clean_args),
                )
            cmd.extend(clean_args)

        if self._verbose:
            self._log_runtime_diagnostics(cmd, effective_cwd, run_env, report_dir)

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

        def _drain(
            stream: Any, sink: list[str], state: list[int], stream_name: str
        ) -> None:
            # readline() is the right primitive here: it returns chunks as
            # they arrive (line-buffered), doesn't block waiting for EOF,
            # and exits cleanly when the pipe closes. read() would buffer
            # the entire stream first, which defeats the purpose for a
            # long-running suite.
            emit_disabled = False

            def _emit(kept: str) -> None:
                # Stream the just-captured text live, mirroring exactly what was
                # appended to ``sink`` so the live view and the final blob agree
                # (and the same cap applies). Blank spacer lines are dropped so
                # they don't spam the log.
                nonlocal emit_disabled
                if on_output is None or emit_disabled:
                    return
                line = kept.rstrip("\r\n")
                if not line:
                    return
                try:
                    on_output(line, stream_name)
                except Exception as exc:  # noqa: BLE001 - a bad sink must never
                    # stop draining (that would block the child on a full pipe):
                    # log once, then stop emitting for this stream and keep
                    # draining into ``sink``.
                    emit_disabled = True
                    _log.warning(
                        "disabling live %s streaming after sink error: %s",
                        stream_name,
                        exc,
                    )

            try:
                for chunk in iter(stream.readline, ""):
                    if max_bytes is None:
                        sink.append(chunk)
                        state[0] += len(chunk)
                        _emit(chunk)
                        continue
                    remaining = max_bytes - state[0]
                    if remaining <= 0:
                        # Already at the cap -- drop the chunk but keep draining
                        # so the child never blocks on a full pipe buffer.
                        continue
                    if len(chunk) <= remaining:
                        sink.append(chunk)
                        state[0] += len(chunk)
                        if state[0] >= max_bytes:
                            state[1] = 1
                        _emit(chunk)
                    else:
                        # The chunk would cross the cap. Keep only what fits so
                        # the budget is a hard ceiling, not "cap + one chunk":
                        # a single very long line from a test must not blow it.
                        kept = chunk[:remaining]
                        sink.append(kept)
                        state[0] = max_bytes
                        state[1] = 1
                        _emit(kept)
            except (ValueError, OSError) as e:
                # The read side failed (pipe closed mid-read, decode error,
                # ...). Distinct from a close() failure below so the log says
                # what actually broke.
                _log.warning("error draining pytest output stream: %s", e)
            finally:
                try:
                    stream.close()
                except Exception as e:  # noqa: BLE001 - best-effort
                    _log.warning(
                        "error closing pytest output stream after drain: %s", e
                    )

        # daemon=True so a stuck drainer never blocks interpreter exit; in
        # practice they always exit because the child closes the pipe.
        stdout_thread = threading.Thread(
            target=_drain,
            args=(proc.stdout, stdout_chunks, stdout_state, "stdout"),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_drain,
            args=(proc.stderr, stderr_chunks, stderr_state, "stderr"),
            daemon=True,
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
        # bounded wait (see _DRAIN_JOIN_TIMEOUT) so a hung drainer cannot wedge
        # the runner forever -- under that condition we lose the tail but the
        # run still returns rather than hanging.
        stdout_thread.join(timeout=self._DRAIN_JOIN_TIMEOUT)
        stderr_thread.join(timeout=self._DRAIN_JOIN_TIMEOUT)

        stdout = "".join(stdout_chunks)
        stderr = "".join(stderr_chunks)
        # The cap is enforced in characters (len(chunk)); see the
        # "Implementation note" in the class docstring. The marker reports the
        # same unit -- "characters" -- to stay consistent with how the budget
        # is actually counted, even though the parameter is spelled
        # ``max_output_bytes`` (bytes == characters for the ASCII output pytest
        # emits almost entirely).
        if stdout_state[1]:
            stdout += (
                f"\n...(stdout truncated at {max_bytes} characters; "
                "increase SubprocessPytestRunner.max_output_bytes to capture more)"
            )
        if stderr_state[1]:
            stderr += (
                f"\n...(stderr truncated at {max_bytes} characters; "
                "increase SubprocessPytestRunner.max_output_bytes to capture more)"
            )

        if timed_out:
            if stdout:
                _log.warning("pytest stdout captured before timeout:\n%s", stdout)
            if stderr:
                _log.warning("pytest stderr captured before timeout:\n%s", stderr)
            # Attach the drained output to the exception too, so callers can
            # surface "why did it hang" programmatically instead of being
            # forced to scrape the worker log.
            raise TestExecutionError(
                f"pytest run timed out after {self._timeout}s",
                stdout=stdout or "",
                stderr=stderr or "",
            )

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

    def _log_runtime_diagnostics(
        self,
        cmd: list[str],
        effective_cwd: str | None,
        run_env: dict[str, str] | None,
        report_dir: str | None,
    ) -> None:
        """Log the fully-resolved pytest invocation (``verbose=True`` only).

        Emits the command, working directory, the env delta against the parent
        ``os.environ`` (keys this run adds vs overrides), and the report dir +
        cleanup policy. Credential-looking *env values* are masked (see
        :func:`_mask_env_value`) so nothing secret reaches the log. Note the
        masking covers env values only: the command (including ``pytest_args``)
        is logged verbatim, so callers must not pass secrets as CLI flags -- use
        ``env`` / ``env_file`` instead. Uses this module's logger, which Airflow
        captures into the task log during ``execute``.
        """
        _log.info("pytest runtime diagnostics -- command: %s", " ".join(cmd))
        _log.info(
            "pytest runtime diagnostics -- cwd: %s",
            effective_cwd if effective_cwd is not None else "<inherited from worker>",
        )
        if run_env:
            added: list[str] = []
            overridden: list[str] = []
            for key in sorted(run_env):
                entry = f"{key}={_mask_env_value(key, run_env[key])}"
                if key not in os.environ:
                    added.append(entry)
                elif run_env[key] != os.environ[key]:
                    overridden.append(entry)
            _log.info(
                "pytest runtime diagnostics -- env added (%d): %s",
                len(added),
                ", ".join(added) or "<none>",
            )
            _log.info(
                "pytest runtime diagnostics -- env overridden vs os.environ (%d): %s",
                len(overridden),
                ", ".join(overridden) or "<none>",
            )
        else:
            _log.info(
                "pytest runtime diagnostics -- env: inherits os.environ unchanged"
            )
        _log.info(
            "pytest runtime diagnostics -- report_dir: %s (cleanup=%r)",
            os.path.abspath(report_dir) if report_dir is not None else "<none>",
            self._cleanup,
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
        # Claim both locations under the lock BEFORE acting. The operator may
        # call cleanup() twice on a kill (once from execute()'s finally, once
        # from on_kill on another thread); claiming up front makes every branch
        # idempotent -- the first caller acts and logs, the second sees None and
        # silently returns (no duplicate logs).
        with self._lock:
            owned = self._created_report_dir
            kept = self._kept_report_dir
            self._created_report_dir = None
            self._kept_report_dir = None

        if owned is not None:
            # The runner's own temp dir -> subject to the cleanup policy.
            if self._cleanup == "never":
                _log.info(
                    "Keeping report directory %s (cleanup='never').",
                    os.path.abspath(owned),
                )
                return
            if self._cleanup == "on_success" and not success:
                _log.info(
                    "Keeping report directory %s for post-mortem "
                    "(cleanup='on_success', run failed).",
                    os.path.abspath(owned),
                )
                return
            _log.info(
                "Removing report directory %s (cleanup=%r).", owned, self._cleanup
            )
            shutil.rmtree(owned, ignore_errors=True)
            return

        if kept is not None:
            # A parser-supplied directory: never removed, regardless of policy.
            # We still log where the report was left, for parity with the
            # owned-temp case (so users see it on failure / with cleanup=never).
            _log.info(
                "Report left at %s (parser-supplied directory; not removed).",
                os.path.abspath(kept),
            )

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
            # Whole group already exited between poll() and now -- nothing to do.
            return
        except OSError as exc:
            # Anything other than "already gone" -- e.g. PermissionError if the
            # child changed its gid out from under us, or a transient ESRCH from
            # two terminators racing (cancel() + timeout). We cannot reach the
            # group, so fall back to killing the direct child best-effort and
            # stop. This must never propagate: _terminate runs on the on_kill /
            # timeout paths where an escaping OSError would mask the real error.
            _log.warning(
                "Could not SIGTERM pytest process group (pid=%d): %s; "
                "falling back to killing the direct child.",
                proc.pid,
                exc,
            )
            self._kill_direct(proc)
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
        except OSError as exc:
            _log.warning(
                "Could not SIGKILL pytest process group (pid=%d): %s; "
                "falling back to killing the direct child.",
                proc.pid,
                exc,
            )
            self._kill_direct(proc)

    @staticmethod
    def _kill_direct(proc: subprocess.Popen[str]) -> None:
        """Best-effort kill of the direct child only (no process group).

        Used as a last resort when the group-signal path fails for a reason
        other than "already gone" (e.g. the child changed gid). The wider tree
        may survive, but at least the child we launched is reaped. Never raises.
        """
        try:
            proc.kill()
        except ProcessLookupError:
            return
        except OSError as exc:  # pragma: no cover - extremely rare
            _log.warning(
                "Direct kill of pytest child (pid=%d) also failed: %s",
                proc.pid,
                exc,
            )

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
