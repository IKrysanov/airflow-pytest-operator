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

from collections.abc import Sequence
from typing import Any, Literal

from ..compat import BaseOperator
from ..exceptions import TestExecutionError, TestsFailedError
from ..models import TestRunResult
from ..reporters import JUnitResultParser, ResultParser
from ..runners import PytestRunner, SubprocessPytestRunner
from ..stores import (
    LastFailedStore,
    VariableLastFailedStore,
    is_final_attempt,
    last_failed_var_key,
)
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
    :param test_retry_strategy: how Airflow task *retries* re-run the suite.
        ``"all"`` (default) re-runs the whole suite on every retry. With
        ``"failed_only"`` a retry re-runs **only** the tests that failed on the
        previous attempt: the failing node-ids are carried between attempts in an
        Airflow Variable (the task's own XCom is cleared on retry; a Variable
        survives and works on Airflow 2.x/3.x) and converted back to selectors
        via :func:`~airflow_pytest_operator.node_id_to_pytest_args`. The Variable
        is consumed on read and (re)written only while a further retry will read
        it -- never on the final/success attempt -- so a killed worker cannot
        orphan it. Best-effort: falls back to the full suite if the backend or
        the ids are unavailable; ``pytest_args`` are never mutated; ignored in
        ``dry_run``. Default: ``"all"``.
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
    :param store: injectable store for the ``failed_only`` cross-retry set
        (default: :class:`~airflow_pytest_operator.stores.VariableLastFailedStore`,
        backed by an Airflow Variable). Any object implementing the
        :class:`~airflow_pytest_operator.stores.LastFailedStore` protocol
        (``read``/``write``/``delete``) works -- structural typing, so no
        subclassing -- which makes a fake (tests) or a custom backend (e.g. a KV
        store) type-check cleanly. Unused unless ``test_retry_strategy="failed_only"``.
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

    def __init__(
        self,
        *,
        test_path: str | Sequence[str],
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        fail_on_test_failure: bool = True,
        dry_run: bool = False,
        test_retry_strategy: Literal["all", "failed_only"] = "all",
        rerun_failed: int = 0,
        runner: PytestRunner | None = None,
        parser: ResultParser | None = None,
        store: LastFailedStore | None = None,
        **kwargs: Any,
    ) -> None:
        if test_retry_strategy not in self._RETRY_STRATEGIES:
            raise ValueError(
                "test_retry_strategy must be one of 'all', 'failed_only'; "
                f"got {test_retry_strategy!r}"
            )
        # Reject bools (a stray True/False is an int subclass) and non-ints
        # up front, so range(self.rerun_failed) can't blow up later.
        if (
            isinstance(rerun_failed, bool)
            or not isinstance(rerun_failed, int)
            or rerun_failed < 0
        ):
            raise ValueError(
                f"rerun_failed must be a non-negative integer; got {rerun_failed!r}"
            )
        # Fail fast on a bad store rather than at the first execute(): the
        # runtime_checkable LastFailedStore protocol lets us reject anything
        # missing read/write/delete right here at init.
        if store is not None and not isinstance(store, LastFailedStore):
            raise TypeError(
                "store must implement the LastFailedStore protocol "
                f"(read/write/delete); got {type(store).__name__}"
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
        self._store: LastFailedStore = store or VariableLastFailedStore()

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

        # failed_only: narrow the run to exactly the tests that failed on the
        # previous attempt, carried between native Airflow retries in an Airflow
        # Variable (see the class docstring for why a Variable rather than this
        # task's own XCom). Narrowing is driven purely by presence: we narrow
        # whenever the store holds a failed set, else run the full ``test_path``.
        # A fresh run starts with nothing stored, so a normal first attempt runs
        # everything; we deliberately don't gate on ``try_number``. Caveat: if a
        # run_id is reused (a cleared/restarted run after a partial crash) a
        # leftover set may narrow even that attempt -- acceptable, it just
        # re-runs the previously-failing subset.
        #
        # CONSUME-ON-READ: the moment we have read the stored failures and turned
        # them into targets, we delete the Variable -- its job is done for this
        # attempt and we already hold everything we need in memory. Deleting now,
        # before the (possibly long, possibly crashing) test run, is what makes
        # cleanup crash-safe: a worker that dies mid-run cannot leave an orphan,
        # because the Variable is already gone. A fresh copy is written at the
        # end only if a further retry will read it (see below). Meaningless in
        # dry-run: --collect-only never runs test bodies, so there's no "last
        # failed" to narrow to and we touch no Variable at all.
        var_key: str | None = None
        targets: str | Sequence[str] = self.test_path
        if self.test_retry_strategy == "failed_only" and not self.dry_run:
            var_key = last_failed_var_key(context)
            if var_key:
                prior = self._store.read(var_key)
                if prior:
                    targets = node_id_to_pytest_args(prior)
                    self._store.delete(var_key)  # consume immediately
                    self.log.info(
                        "test_retry_strategy='failed_only' -- narrowing to the %d "
                        "test(s) that failed on the previous attempt, carried via "
                        "Airflow Variable %r (now consumed).",
                        len(targets),
                        var_key,
                    )

        run_ok = False
        try:
            # First pass: run the (possibly failed_only-narrowed) target set and
            # snapshot its summary right away. That snapshot is the honest
            # picture of the suite that goes to XCom (Airflow pushes it under the
            # standard "return_value" key when do_xcom_push is on), even if the
            # in-process reruns below recover some failures. ``result`` then
            # tracks the latest run; the snapshot is read back from ``summary``.
            result = self._run_and_parse(targets, effective_args)
            summary = dict(result.to_xcom())
            still_failing = list(result.failed_node_ids)
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
                    self._runner.cleanup(success=False)
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

            # ``summary`` already holds the first full run's counts (snapshotted
            # above); ``result`` is the latest run. Only when reruns happened do
            # we add the post-rerun view so the final outcome is unambiguous.
            run_ok = result.success
            if rerun_rounds:
                recovered = [
                    nid
                    for nid in summary["failed_node_ids"]
                    if nid not in still_failing
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

            # failed_only: hand the still-failing set forward to the next
            # attempt -- but ONLY when there will be one. We write the Variable
            # exclusively when this attempt failed AND Airflow will retry it
            # (not the final attempt). On success, or on the final attempt, we
            # write nothing, so the terminal attempt never leaves a Variable
            # behind -- even if the worker is killed right after this point,
            # there is simply nothing to clean up. (Combined with consume-on-read
            # above, the Variable exists only in the gap between a failed
            # non-final attempt and the retry that consumes it.) Written before
            # the raise below so the failing attempt hands its failures forward.
            if (
                var_key is not None
                and still_failing
                and not is_final_attempt(context)
            ):
                self._store.write(var_key, still_failing)

            if self.fail_on_test_failure and not run_ok:
                raise TestsFailedError(result)

            return summary
        finally:
            # Always invoke cleanup; the runner decides what to remove based on
            # its policy and the success flag, and its cleanup is best-effort
            # (never raises), so we call it directly. The failed_only Variable is
            # NOT touched here on purpose: it is consumed on read and only
            # (re)written when a retry will read it, so there is no teardown-time
            # delete that a crash could skip.
            self._runner.cleanup(success=run_ok)

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
