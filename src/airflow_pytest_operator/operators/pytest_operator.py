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
from ._constants import (
    COLLECT_ONLY_ALIASES,
    DIST_FLAGS,
    KEYWORD_FLAGS,
    MARKER_FLAGS,
    MAX_STDERR_LEN,
    NUMPROCESSES_FLAGS,
    has_flag,
)
from ._coverage import CoverageController
from ._validation import (
    validate_cov_fail_under,
    validate_coverage,
    validate_env,
    validate_markers_keyword,
    validate_parallel_dist,
    validate_rerun_failed,
    validate_store,
    validate_test_retry_strategy,
)


class PytestOperator(BaseOperator):
    """Run a pytest suite as an Airflow task.

    Orchestrates an injectable runner -> parser and handles the Airflow side
    (templating, XCom, fail policy, retries). ``execute`` returns a summary dict
    pushed to XCom under ``return_value`` (unless ``do_xcom_push=False``). See the
    README for full behaviour and examples.

    :param test_path: pytest target(s) -- file, directory, or node-id, or a
        sequence of them (templated).
    :param pytest_args: extra pytest CLI args, e.g. ``["-x", "-q"]`` (templated).
    :param markers: ``-m`` marker expression (templated). Sugar over
        ``pytest_args``; defers to an explicit ``-m`` and skipped if empty.
    :param keyword: ``-k`` keyword expression (templated). Same rules as
        ``markers``.
    :param env: extra environment variables for the run (templated).
    :param env_file: path to a ``.env`` merged into the child (templated;
        precedence ``os.environ`` < ``env_file`` < ``env``). ``AIRFLOW*`` keys
        skipped unless ``env_file_overrides``. Needs the ``[dotenv]`` extra.
    :param env_file_overrides: let ``env_file`` set ``AIRFLOW*`` keys (default
        False; the explicit ``env`` is never restricted).
    :param fail_on_test_failure: fail the task on any test failure/error. If
        False, the task always succeeds and the outcome lives only in XCom.
    :param dry_run: invoke pytest with ``--collect-only`` (collect, run no test
        bodies). A collection error still fails the task. Default False.
    :param test_retry_strategy: how Airflow retries re-run the suite -- ``"all"``
        (default) or ``"failed_only"`` (only the prior attempt's failures, carried
        in an Airflow Variable). Best-effort; ignored in ``dry_run``.
    :param rerun_failed: extra in-process rounds re-running only the failed tests
        (no pytest cache, no Airflow retry). ``0`` disables. Adds ``rerun_rounds``
        / ``recovered_node_ids`` / ``still_failing_node_ids`` to the summary.
        Ignored in ``dry_run``. Non-negative int.
    :param parallel: pytest-xdist worker count -- int, or ``"auto"``/``"logical"``
        (maps to ``-n``). First full run only; ignored in ``dry_run``; defers to
        an explicit ``-n``. Needs the ``[xdist]`` extra. Default None (serial).
    :param dist: pytest-xdist ``--dist`` mode (``"load"``, ``"loadscope"``,
        ``"loadfile"``, ``"loadgroup"``, ``"worksteal"``, ``"each"``, ``"no"``).
        Requires ``parallel``. Default None.
    :param stream_output: when True (default), child stdout/stderr is logged
        line-by-line as it runs (unbuffered ``-u``); False logs one blob at the
        end. The full output is captured either way.
    :param coverage: when True, measure coverage via pytest-cov on the first full
        run and push the overall fraction to XCom under ``coverage`` (a float in
        ``[0, 1]``, or ``None``). Defers to a user ``--cov``/``--no-cov``, skipped
        in ``dry_run``, not applied to reruns. Needs the ``[coverage]`` extra.
        See the README. Default False.
    :param cov_fail_under: optional coverage gate -- a fraction in ``[0, 1]``
        compared against the measured ``coverage``. Enables measurement
        automatically and fails the task with :class:`CoverageThresholdError`
        below the threshold (test failures take precedence; fail-closed if
        unmeasurable). See the README. Default None (no gate).
    :param store: injectable ``LastFailedStore`` for ``failed_only`` (default
        ``VariableLastFailedStore``). Unused unless
        ``test_retry_strategy="failed_only"``.
    :param runner: injectable :class:`PytestRunner` (default: subprocess).
    :param parser: injectable :class:`ResultParser` (default: JUnit).
    """

    # Airflow Jinja-templates these attributes before execute() runs.
    template_fields: Sequence[str] = (
        "test_path",
        "pytest_args",
        "env",
        "env_file",
        "markers",
        "keyword",
    )
    ui_color = "#4caf50"

    def __init__(
        self,
        *,
        test_path: str | Sequence[str],
        pytest_args: Sequence[str] | None = None,
        markers: str | None = None,
        keyword: str | None = None,
        env: dict[str, str] | None = None,
        env_file: str | None = None,
        env_file_overrides: bool = False,
        fail_on_test_failure: bool = True,
        dry_run: bool = False,
        test_retry_strategy: Literal["all", "failed_only"] = "all",
        rerun_failed: int = 0,
        parallel: int | str | None = None,
        dist: str | None = None,
        stream_output: bool = True,
        coverage: bool = False,
        cov_fail_under: float | None = None,
        runner: PytestRunner | None = None,
        parser: ResultParser | None = None,
        store: LastFailedStore | None = None,
        **kwargs: Any,
    ) -> None:
        # Validate constructor arguments (extracted to _validation.py).
        # Convention: wrong *type* -> TypeError, valid type / wrong *value* ->
        # ValueError. Each call raises with a message naming the bad argument.
        validate_test_retry_strategy(test_retry_strategy)
        validate_markers_keyword(markers, keyword)
        validate_rerun_failed(rerun_failed)
        validate_parallel_dist(parallel, dist)
        validate_coverage(coverage)
        validate_cov_fail_under(cov_fail_under)
        validate_store(store)
        validate_env(env)

        super().__init__(**kwargs)
        self.test_path = test_path
        self.pytest_args = list(pytest_args) if pytest_args else []
        self.markers = markers
        self.keyword = keyword
        self.env = env or {}
        self.env_file = env_file
        self.env_file_overrides = env_file_overrides
        self.fail_on_test_failure = fail_on_test_failure
        self.dry_run = dry_run
        self.test_retry_strategy = test_retry_strategy
        self.rerun_failed = rerun_failed
        self.parallel = parallel
        self.dist = dist
        self.stream_output = stream_output
        self.coverage = coverage
        # Store as float so the gate comparison and message formatting are
        # uniform (an int like 1 or 0 is a valid fraction).
        self.cov_fail_under = (
            float(cov_fail_under) if cov_fail_under is not None else None
        )
        # The coverage concern (splice flags, read the fraction, gate) lives in
        # its own controller so the operator only logs + builds the XCom summary.
        # Built from the stored attributes (normalized) so the controller always
        # mirrors the operator's canonical state.
        self._coverage = CoverageController(
            coverage=self.coverage, cov_fail_under=self.cov_fail_under
        )
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
            arg in COLLECT_ONLY_ALIASES for arg in effective_args
        ):
            effective_args.append("--collect-only")

        # markers / keyword: sugar for -m / -k on the first full run. Skipped if
        # empty; defers to the same flag already in pytest_args.
        for name, value, flags in (
            ("markers", self.markers, MARKER_FLAGS),
            ("keyword", self.keyword, KEYWORD_FLAGS),
        ):
            if not (value and value.strip()):
                continue
            if has_flag(effective_args, flags):
                self.log.warning(
                    "%s=%r ignored: pytest_args already sets %s; deferring to "
                    "your explicit arg.",
                    name,
                    value,
                    flags[0],
                )
            else:
                effective_args += [flags[0], value]

        # pytest-cov: the controller owns the splice/active/defer decision.
        # ``cov_active`` -> a coverage total will be produced; ``cov_deferred``
        # -> the user already drives --cov/--no-cov, so we warn.
        cov_active, cov_deferred = self._coverage.augment_args(
            effective_args, dry_run=self.dry_run
        )
        if cov_deferred:
            self.log.warning(
                "coverage measurement (coverage=%r / cov_fail_under=%r) ignored: "
                "pytest_args already sets --cov / --no-cov; deferring to your "
                "explicit arg.",
                self.coverage,
                self.cov_fail_under,
            )

        # pytest-xdist: -n / --dist on the first full run only (skipped in
        # dry-run). Defer to a user-supplied -n/--numprocesses (and skip --dist).
        if not self.dry_run and self.parallel is not None:
            if has_flag(effective_args, NUMPROCESSES_FLAGS):
                self.log.warning(
                    "parallel=%r / dist=%r ignored: pytest_args already sets "
                    "-n/--numprocesses; deferring to your explicit args.",
                    self.parallel,
                    self.dist,
                )
            else:
                effective_args += ["-n", str(self.parallel)]
                # Defer to a user-supplied --dist (xdist keeps the last one).
                if self.dist is not None:
                    if has_flag(effective_args, DIST_FLAGS):
                        self.log.warning(
                            "dist=%r ignored: pytest_args already sets --dist; "
                            "deferring to your explicit arg.",
                            self.dist,
                        )
                    else:
                        effective_args += ["--dist", self.dist]

        # failed_only: narrow to the prior attempt's failures, carried in a
        # Variable. Consume-on-read (delete before the run) so a dead worker
        # can't orphan it; a fresh copy is written at the end only if a retry
        # will read it. No-op in dry-run.
        var_key: str | None = None
        targets: str | Sequence[str] = self.test_path
        if self.test_retry_strategy == "failed_only" and not self.dry_run:
            var_key = last_failed_var_key(context)
            if var_key:
                prior = self._safe_store_read(var_key)
                if prior:
                    targets = node_id_to_pytest_args(prior)
                    self._safe_delete_store(var_key)  # consume immediately
                    self.log.info(
                        "test_retry_strategy='failed_only' -- narrowing to the %d "
                        "test(s) that failed on the previous attempt, carried via "
                        "Airflow Variable %r (now consumed).",
                        len(targets),
                        var_key,
                    )

        run_ok = False
        try:
            # First pass: snapshot the summary now -- the honest picture pushed
            # to XCom even if the reruns below recover failures.
            result, coverage_percent = self._run_and_parse(
                targets, effective_args, measure_coverage=cov_active
            )
            summary = dict(result.to_xcom())
            # Surface the fraction only when coverage was active -- keeps the XCom
            # shape unchanged otherwise. From the first run; not re-derived.
            if cov_active:
                summary["coverage"] = coverage_percent
                if coverage_percent is None:
                    self.log.warning(
                        "coverage was requested but no TOTAL row was found in "
                        "pytest's output -- pushing coverage=None. Make sure "
                        "--cov-report includes a terminal report (term / "
                        "term-missing) and that the run produced coverage data."
                    )
                else:
                    self.log.info(
                        "Overall coverage %.2f pushed to XCom under 'coverage'.",
                        coverage_percent,
                    )
            still_failing = list(result.failed_node_ids)
            rerun_rounds = 0

            # In-process reruns of only the failed tests -- no pytest cache, no
            # Airflow retry. Skipped in dry-run and when nothing failed.
            if not self.dry_run and self.rerun_failed > 0 and still_failing:
                for _ in range(self.rerun_failed):
                    if not still_failing:
                        break
                    # Free the previous run's report dir so rounds don't leak.
                    self._safe_cleanup(success=False)
                    rerun_rounds += 1
                    selectors = node_id_to_pytest_args(still_failing)
                    self.log.info(
                        "Rerun %d/%d: re-running %d previously-failed test(s)",
                        rerun_rounds,
                        self.rerun_failed,
                        len(selectors),
                    )
                    result, _ = self._run_and_parse(selectors, list(self.pytest_args))
                    still_failing = list(result.failed_node_ids)

            # Add the post-rerun view to the snapshot when reruns happened.
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

            # failed_only: hand the still-failing set forward, but only when a
            # next attempt will read it (this attempt fails the task and isn't
            # the final one) -- so no terminal attempt orphans a Variable.
            if (
                var_key is not None
                and still_failing
                and self.fail_on_test_failure
                and not is_final_attempt(context, log=self.log)
            ):
                self._safe_store_write(var_key, still_failing)

            if self.fail_on_test_failure and not run_ok:
                raise TestsFailedError(result)

            # Coverage gate: the controller raises below the threshold
            # (fail-closed if unmeasurable). After the test-failure raise above,
            # so a red suite reports that first.
            if cov_active and self._coverage.gate_enabled:
                self._coverage.evaluate_gate(coverage_percent)
                summary["coverage_passed"] = True
                self.log.info(
                    "Coverage gate passed: %.2f >= cov_fail_under=%.2f.",
                    coverage_percent,
                    self.cov_fail_under,
                )

            return summary
        finally:
            # Always clean up (wrapped so a cleanup error never masks the real
            # outcome). The failed_only Variable is untouched here on purpose.
            self._safe_cleanup(success=run_ok)

    def _run_and_parse(
        self,
        targets: str | Sequence[str],
        pytest_args: Sequence[str],
        *,
        measure_coverage: bool = False,
    ) -> tuple[TestRunResult, float | None]:
        """Run pytest once against ``targets`` and parse the report.

        Shared by the first full run and each in-process rerun. Returns the
        parsed :class:`TestRunResult` plus the coverage fraction (a float in
        ``[0, 1]`` when ``measure_coverage`` is set and a ``TOTAL`` row is found,
        else ``None``). A missing report raises :class:`TestExecutionError`.
        """
        # stream_output: a live sink so pytest output reaches the task log
        # line-by-line; the full output is still returned in ``artifacts``.
        on_output = self._emit_pytest_line if self.stream_output else None
        artifacts = self._runner.run(
            targets,
            pytest_args=pytest_args,
            env=self.env,
            # env_file is a path; the runner reads/merges it (no fs work here).
            env_file=self.env_file,
            env_file_overrides=self.env_file_overrides,
            # The parser owns the report flags/location; the runner just splices.
            report_request=self._parser.report_request,
            on_output=on_output,
        )

        # Surface child output in the task log. When streaming, the lines were
        # already logged live (above), so the end-of-run blob would just be a
        # duplicate -- skip it. When not streaming, log the blob as before.
        if not self.stream_output:
            if artifacts.stdout:
                self.log.info("pytest stdout:\n%s", artifacts.stdout)
            if artifacts.stderr:
                self.log.warning("pytest stderr:\n%s", artifacts.stderr)

        # No report -> pytest never wrote one (collection error / crash before
        # any test ran). An execution failure -- surface the captured stderr.
        if artifacts.report_path is None:
            stderr_text = artifacts.stderr or "<empty>"
            if len(stderr_text) > MAX_STDERR_LEN:
                stderr_text = stderr_text[:MAX_STDERR_LEN] + "...(truncated)"

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

        # Coverage is read by the controller from the captured stdout, only on
        # the run that measured it.
        coverage = (
            self._coverage.extract(artifacts.stdout) if measure_coverage else None
        )
        return result, coverage

    def _emit_pytest_line(self, line: str, stream: str) -> None:
        """Live sink (``on_output``) routing each child line to the task log.

        stdout -> info, stderr -> warning, so output streams to the UI live.
        """
        if stream == "stderr":
            self.log.warning("%s", line)
        else:
            self.log.info("%s", line)

    # -- best-effort collaborator calls ---------------------------------
    #
    # runner/store are injection points and could raise; these wrappers keep a
    # bookkeeping/teardown error from masking execute()'s real outcome.

    def _safe_cleanup(self, *, success: bool) -> None:
        """Invoke the runner's cleanup; teardown must never mask the outcome."""
        try:
            self._runner.cleanup(success=success)
        except Exception:
            self.log.exception("Error while cleaning up report directory")

    def _safe_store_read(self, key: str) -> list[str]:
        """Read the prior failed set; a store error degrades to the full suite."""
        try:
            return self._store.read(key)
        except Exception:
            self.log.exception("Error while reading failed_only Variable %r", key)
            return []

    def _safe_delete_store(self, key: str) -> None:
        """Consume (delete) the failed_only Variable; never raise into execute()."""
        try:
            self._store.delete(key)
        except Exception:
            self.log.exception("Error while deleting failed_only Variable %r", key)

    def _safe_store_write(self, key: str, node_ids: list[str]) -> None:
        """Hand the failed set forward; a store error must not mask the result."""
        try:
            self._store.write(key, node_ids)
        except Exception:
            self.log.exception("Error while writing failed_only Variable %r", key)

    def on_kill(self) -> None:
        """Abort the run when Airflow terminates the task (timeout / clear / SIGTERM).

        Delegates to the runner, which owns the process; a runner with nothing
        to cancel inherits a safe no-op.
        """
        self.log.warning("Task killed -- cancelling pytest run on %s", self.test_path)
        try:
            self._runner.cancel()
        except Exception:  # pragma: no cover - best-effort teardown
            # on_kill must never raise -- it runs during teardown.
            self.log.exception("Error while cancelling pytest run")

        # Kills/timeouts are exactly when temp dirs leak, so clean here too.
        # cleanup() is idempotent, so racing execute()'s finally is harmless.
        try:
            self._runner.cleanup(success=False)
        except Exception:  # pragma: no cover - best-effort teardown
            self.log.exception("Error while cleaning up report directory")
