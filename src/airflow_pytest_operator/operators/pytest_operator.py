"""The Airflow operator.

This class is intentionally *thin*. Its only responsibilities are:
  1. orchestrate runner -> parser,
  2. integrate with Airflow (templating, XCom, logging, fail policy).

It contains no subprocess logic and no XML parsing. Both collaborators
are injected (Dependency Inversion): defaults are provided for ergonomic
use in DAGs, but tests can pass fakes, and advanced users can swap in a
Docker/K8s runner or a JSON parser without subclassing.
"""

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
import re
import tempfile
from collections.abc import Sequence
from typing import Any

from ..compat import BaseOperator
from ..exceptions import TestExecutionError, TestsFailedError
from ..models import TestRunResult
from ..reporters import JUnitResultParser, ResultParser
from ..runners import PytestRunner, SubprocessPytestRunner
from ..utils import node_id_to_pytest_args


class PytestOperator(BaseOperator):
    """Run a pytest suite as an Airflow task.

    :param test_path: target(s) to pass to pytest -- a single file,
        directory, or node-id selector, or a sequence of them (templated).
    :param pytest_args: extra CLI args, e.g. ``["-k", "smoke", "-x"]`` (templated).
    :param env: extra environment variables for the run (templated).
    :param fail_on_test_failure: if True (default) the task fails when any
        test fails or errors; if False the task always succeeds and the
        outcome is only reflected in XCom.
    :param dry_run: when True, pytest is invoked with ``--collect-only``.
        Test bodies are NOT executed; only collection runs. Useful as a
        pre-flight task in a DAG: verifies the test path resolves on the
        worker, imports succeed (so worker has all required deps), and
        collection itself succeeds (no syntax errors, no missing fixtures).
        Note that ``--collect-only`` still imports the test modules and
        runs their module-level code, so this is "no tests are executed",
        not "nothing happens". Collection errors surface the same way
        normal failures do -- exit code is non-zero, ``TestRunResult.success``
        is False, and (with the default ``fail_on_test_failure=True``) the
        task fails. Default: False.
    :param test_retry_strategy: how Airflow task *retries* re-run the
        suite. ``"all"`` (default) re-runs the whole suite on every retry --
        unchanged behaviour. ``"failed_only"`` appends pytest's ``--lf``
        (``--last-failed``) on retry attempts (``try_number > 1``), so only
        the tests that failed on the previous attempt run again; this can
        cut retry time dramatically on large suites where only a few tests
        failed. The first attempt always runs the full suite. ``--lf`` is
        backed by pytest's cache (``.pytest_cache``), so it only narrows the
        run when that cache from the previous attempt is still readable on
        the worker; otherwise pytest safely falls back to running everything.
        The user's ``pytest_args`` are not mutated -- the flag is appended to
        a per-call effective list at ``execute()`` time, and is not added if
        ``--lf``/``--last-failed`` is already present. Default: ``"all"``.
        Note this is **best-effort**: ``--lf`` depends on the worker's
        ``.pytest_cache`` (it degrades to a full run on a fresh worker, e.g. a
        retry that lands on a different K8s/Celery pod, and can race between
        parallel tasks that share a pytest rootdir). For a cache-independent
        guarantee on any executor, carry ``failed_node_ids`` between two tasks
        via XCom and convert them with
        :func:`~airflow_pytest_operator.node_id_to_pytest_args` (the
        run-all -> run-failed pattern in the README).
    :param rerun_failed: number of extra **in-process** rounds to re-run
        *only* the tests that failed, before reporting the final outcome.
        ``0`` (default) disables it -- the suite runs once, behaviour
        unchanged. With e.g. ``2`` the operator runs the full suite, then
        re-runs only the still-failing tests up to two more times (stopping
        early as soon as none fail), all within this single task execution.
        Unlike ``test_retry_strategy``/``--lf`` this needs no pytest cache and
        no Airflow retry, so it is robust on any executor (Local/Celery/
        Kubernetes) and deterministic. When reruns happen the XCom summary
        keeps the first full run's counts and adds ``rerun_rounds``,
        ``recovered_node_ids`` and ``still_failing_node_ids``; the task fails
        only if tests still fail after all rounds. Ignored in ``dry_run`` mode.
        Must be a non-negative integer.
    :param runner: injectable :class:`PytestRunner` (default: subprocess).
    :param parser: injectable :class:`ResultParser` (default: JUnit). The
        parser owns the report location: set ``report_dir`` on it, e.g.
        ``JUnitResultParser(report_dir="/opt/airflow/artifacts")``, to choose
        where the report lands. If omitted, the runner writes to a temporary
        directory it cleans up per its ``cleanup`` policy. This is independent
        of which runner you inject -- the location travels with the parser.

    The structured summary is returned from ``execute`` and therefore pushed
    to XCom under the standard ``return_value`` key. To disable that, pass
    Airflow's standard ``do_xcom_push=False`` (no custom flag needed). Read
    the summary downstream with ``xcom_pull(task_ids="<task>")``.
    """

    # Airflow Jinja-templates these attributes before execute() runs.
    template_fields: Sequence[str] = ("test_path", "pytest_args", "env")
    ui_color = "#4caf50"

    _MAX_STDERR_LEN = 4096

    _COLLECT_ONLY_ALIASES: frozenset[str] = frozenset(
        {"--collect-only", "--collectonly", "--co"}
    )

    _RETRY_STRATEGIES: frozenset[str] = frozenset({"all", "failed_only"})

    _LAST_FAILED_ALIASES: frozenset[str] = frozenset({"--lf", "--last-failed"})

    def __init__(
        self,
        *,
        test_path: str | Sequence[str],
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        fail_on_test_failure: bool = True,
        dry_run: bool = False,
        test_retry_strategy: str = "all",
        rerun_failed: int = 0,
        runner: PytestRunner | None = None,
        parser: ResultParser | None = None,
        **kwargs: Any,
    ) -> None:
        if test_retry_strategy not in self._RETRY_STRATEGIES:
            raise ValueError(
                "test_retry_strategy must be one of 'all', 'failed_only'; "
                f"got {test_retry_strategy!r}"
            )
        if rerun_failed < 0:
            raise ValueError(
                f"rerun_failed must be a non-negative integer; got {rerun_failed!r}"
            )
        super().__init__(**kwargs)
        self.test_path = test_path
        self.pytest_args = list(pytest_args) if pytest_args else []
        self.env = env or {}
        self.fail_on_test_failure = fail_on_test_failure
        self.dry_run = dry_run
        self.test_retry_strategy = test_retry_strategy
        self.rerun_failed = rerun_failed
        self._runner = runner or SubprocessPytestRunner()
        self._parser = parser or JUnitResultParser()

    def execute(self, context: Any) -> dict[str, Any]:
        if self.dry_run:
            self.log.info(
                "Running pytest in dry-run mode (--collect-only) on %s -- "
                "tests will be collected but their bodies will NOT run. "
                "Module-level code and collection-time fixtures still execute.",
                self.test_path,
            )
        else:
            self.log.info("Running pytest on %s", self.test_path)

        effective_args = list(self.pytest_args)
        if self.dry_run and not any(
            arg in self._COLLECT_ONLY_ALIASES for arg in effective_args
        ):
            effective_args.append("--collect-only")

        # For failed_only, point pytest at a per-task-instance cache directory
        # so (a) parallel tasks that share a rootdir don't clobber each other's
        # "last failed" record, and (b) the directory is stable across THIS
        # task's retries so --lf on a retry finds the previous attempt's
        # failures. Injected on every attempt (attempt 1 must write it); skipped
        # if the user already set a cache_dir. Stored for cleanup below.
        cache_dir: str | None = None
        if self.test_retry_strategy == "failed_only":
            derived = self._failed_only_cache_dir(context)
            if derived and not any("cache_dir=" in arg for arg in effective_args):
                effective_args += ["-o", f"cache_dir={derived}"]
                # Only track it for cleanup when WE injected it -- never touch a
                # user-supplied cache_dir.
                cache_dir = derived

            # On retries, narrow the run to only the tests that failed on the
            # previous attempt (pytest --lf). The first attempt runs the full
            # suite; we don't double-add the flag if the user already passed it.
            if self._is_retry(context) and not any(
                arg in self._LAST_FAILED_ALIASES for arg in effective_args
            ):
                effective_args.append("--lf")
                self.log.info(
                    "Retry attempt detected with test_retry_strategy='failed_only' "
                    "-- appending --lf so pytest re-runs only the tests that failed "
                    "on the previous attempt (falls back to the full suite if the "
                    "pytest cache is unavailable on this worker)."
                )

        run_ok = False
        try:
            # First pass: run the full target set.
            first = self._run_and_parse(self.test_path, effective_args)
            result = first
            still_failing = list(first.failed_node_ids)
            rerun_rounds = 0

            # In-process reruns of ONLY the failed tests. This needs no pytest
            # cache and no Airflow retry, so it is robust on any executor: the
            # set of failures is carried in memory across rounds within this
            # single execute(). Skipped in dry-run (there are no test bodies to
            # fail) and when nothing failed.
            if not self.dry_run and self.rerun_failed > 0 and still_failing:
                for _ in range(self.rerun_failed):
                    if not still_failing:
                        break
                    # Free the just-finished run's report dir before the next
                    # run so sequential rounds don't leak temp directories.
                    self._safe_cleanup(success=False)
                    rerun_rounds += 1
                    selectors = node_id_to_pytest_args(still_failing)
                    self.log.info(
                        "Rerun %d/%d: re-running %d previously-failed test(s)",
                        rerun_rounds,
                        self.rerun_failed,
                        len(selectors),
                    )
                    result = self._run_and_parse(selectors, list(self.pytest_args))
                    still_failing = list(result.failed_node_ids)

            # The summary is returned from execute(); Airflow pushes it to
            # XCom under the standard "return_value" key when do_xcom_push is
            # on (the default). We keep the first full run's counts (the honest
            # picture of the suite) and, only when reruns happened, add the
            # post-rerun view so the final outcome is unambiguous.
            summary = dict(first.to_xcom())
            run_ok = result.success
            if rerun_rounds:
                recovered = [
                    nid for nid in first.failed_node_ids if nid not in still_failing
                ]
                summary["success"] = run_ok
                summary["rerun_rounds"] = rerun_rounds
                summary["recovered_node_ids"] = recovered
                summary["still_failing_node_ids"] = still_failing
                self.log.info(
                    "After %d rerun round(s): recovered=%d, still failing=%d",
                    rerun_rounds,
                    len(recovered),
                    len(still_failing),
                )

            if self.fail_on_test_failure and not run_ok:
                raise TestsFailedError(result)

            return summary
        finally:
            # Always invoke cleanup; the runner decides what to remove
            # based on its policy and the success flag. Never let cleanup
            # errors mask the real outcome of execute().
            self._safe_cleanup(success=run_ok)
            # Remove the per-task pytest cache dir per the runner's policy.
            # We hand the runner both the outcome and whether this is the final
            # attempt: it cleans once no further retry will read the cache (on
            # success, or on the last attempt even if it failed), and keeps it
            # while more retries remain. No-op unless failed_only injected one.
            if cache_dir is not None:
                self._safe_clean_cache(
                    cache_dir,
                    success=run_ok,
                    terminal=self._is_final_attempt(context),
                )

    def _run_and_parse(
        self, targets: str | Sequence[str], pytest_args: Sequence[str]
    ) -> TestRunResult:
        """Run pytest once against ``targets`` and parse the report.

        Shared by the first full run and each in-process rerun. Surfaces
        child output, turns a missing report into a clear
        :class:`TestExecutionError`, parses the result, and logs the summary.
        """
        artifacts = self._runner.run(
            targets,
            pytest_args=pytest_args,
            env=self.env,
            # The parser decides which pytest flags to add and where the
            # report will land; the runner just splices and reports back.
            # This is what keeps the runner format-agnostic.
            report_request=self._parser.report_request,
        )

        # Surface child output in the task log regardless of outcome.
        if artifacts.stdout:
            self.log.info("pytest stdout:\n%s", artifacts.stdout)
        if artifacts.stderr:
            self.log.warning("pytest stderr:\n%s", artifacts.stderr)

        # No report means pytest never got far enough to write one
        # (collection error, internal crash, OOM kill, wrong path,
        # missing report-plugin for the configured parser).
        # This is an *execution* failure, not a test failure -- surface
        # it clearly with the captured stderr, not a cryptic parse error.
        if artifacts.report_path is None:
            stderr_text = artifacts.stderr or "<empty>"
            if len(stderr_text) > self._MAX_STDERR_LEN:
                stderr_text = stderr_text[: self._MAX_STDERR_LEN] + "...(truncated)"

            raise TestExecutionError(
                f"pytest produced no report for {type(self._parser).__name__} "
                f"(exit code {artifacts.exit_code}). "
                "This usually means a collection error or crash before "
                "any test ran. Captured stderr:\n"
                f"{stderr_text}"
            )

        result = self._parser.parse(
            artifacts.report_path, exit_code=artifacts.exit_code
        )

        self.log.info(
            "Results: total=%d passed=%d failed=%d errors=%d skipped=%d (%.2fs)",
            result.total,
            result.passed,
            result.failed,
            result.errors,
            result.skipped,
            result.duration,
        )
        if result.failed_node_ids:
            self.log.error("Failed tests:\n  %s", "\n  ".join(result.failed_node_ids))
        return result

    def _safe_cleanup(self, *, success: bool) -> None:
        """Invoke the runner's cleanup; teardown must never raise.

        Used both between in-process reruns (to free each round's report dir)
        and in ``execute``'s ``finally``.
        """
        try:
            self._runner.cleanup(success=success)
        except Exception:  # pragma: no cover - best-effort teardown
            self.log.exception("Error while cleaning up report directory")

    def _safe_clean_cache(
        self, cache_dir: str, *, success: bool, terminal: bool
    ) -> None:
        """Ask the runner to clean the pytest cache dir; never raise."""
        try:
            self._runner.clean_pytest_cache(
                cache_dir, success=success, terminal=terminal
            )
        except Exception:  # pragma: no cover - best-effort teardown
            self.log.exception("Error while cleaning up pytest cache directory")

    @staticmethod
    def _is_final_attempt(context: Any) -> bool:
        """True when Airflow will NOT retry the task after this attempt.

        Derived from ``try_number`` and ``max_tries`` on the task instance: the
        final attempt is the one whose ``try_number`` exceeds ``max_tries``
        (e.g. ``retries=2`` -> ``max_tries=2`` with attempts ``try_number``
        1, 2, 3; the third is final). Read **defensively**: if either value is
        missing or not an int we return ``False`` (treat as "more retries may
        come"). Erring toward *keeping* the cache is the safe default -- a wrong
        guess then only costs the next retry its ``--lf`` speed-up, never
        correctness -- which also contains the well-known fragility of
        ``try_number`` semantics across Airflow versions.
        """
        ti = context.get("ti") if hasattr(context, "get") else None
        try_number = getattr(ti, "try_number", None)
        max_tries = getattr(ti, "max_tries", None)
        if not (isinstance(try_number, int) and isinstance(max_tries, int)):
            return False
        return try_number > max_tries

    @staticmethod
    def _failed_only_cache_dir(context: Any) -> str | None:
        """A stable, per-task-instance pytest cache dir for ``failed_only``.

        Derived from the Airflow ids ``(dag_id, task_id, run_id)`` -- and
        crucially **not** ``try_number`` -- so it is the same path across this
        task's retries (``--lf`` finds the previous attempt's failures) yet
        unique per task instance (parallel tasks sharing a rootdir no longer
        clobber each other's ``.pytest_cache``). Returns ``None`` when the ids
        are unavailable, so the operator simply falls back to pytest's default
        cache location rather than guessing.
        """
        ti = context.get("ti") if hasattr(context, "get") else None
        dag_id = getattr(ti, "dag_id", None)
        task_id = getattr(ti, "task_id", None)
        run_id = getattr(ti, "run_id", None)
        if not (
            isinstance(dag_id, str)
            and isinstance(task_id, str)
            and isinstance(run_id, str)
        ):
            return None
        key = re.sub(r"[^0-9A-Za-z._-]+", "_", f"{dag_id}__{task_id}__{run_id}")
        return os.path.join(tempfile.gettempdir(), "apo_pytest_cache", key)

    @staticmethod
    def _is_retry(context: Any) -> bool:
        """True when Airflow is re-running this task (a retry attempt).

        Airflow exposes the attempt number on the task instance as
        ``try_number``: 1 on the first attempt, 2+ on each retry. We read it
        defensively (``getattr`` with a default of 1) so a missing or
        unusual context degrades to "first attempt" -- i.e. the full suite
        runs -- rather than raising during execute().
        """
        ti = context.get("ti") if hasattr(context, "get") else None
        try_number = getattr(ti, "try_number", 1)
        return isinstance(try_number, int) and try_number > 1

    def on_kill(self) -> None:
        """Abort the test run when Airflow terminates the task.

        Airflow calls this when the task is killed -- execution timeout,
        a manual clear/mark-failed, or the worker shutting down (SIGTERM).
        We delegate to the runner, which owns the actual process/resource;
        the operator deliberately knows nothing about subprocesses.

        Delegation keeps responsibilities separate: the operator handles
        the Airflow lifecycle, the runner handles teardown of whatever it
        spawned. Runners that have nothing to cancel inherit a safe no-op.
        """
        self.log.warning("Task killed -- cancelling pytest run on %s", self.test_path)
        try:
            self._runner.cancel()
        except Exception:  # pragma: no cover - best-effort teardown
            # on_kill must never raise: it runs during teardown, and an
            # exception here can mask the original termination cause and
            # leave the task in a confusing state.
            self.log.exception("Error while cancelling pytest run")

        # A killed run is never successful, but with the default "always"
        # policy the temp report dir is still removed -- kills/timeouts are
        # exactly when leaked dirs pile up, so we must clean here too. The
        # cancel() above has already stopped the process, so nothing is
        # still writing into the directory. cleanup() is idempotent and
        # thread-safe, so racing with execute()'s own finally is harmless.
        try:
            self._runner.cleanup(success=False)
        except Exception:  # pragma: no cover - best-effort teardown
            self.log.exception("Error while cleaning up report directory")
