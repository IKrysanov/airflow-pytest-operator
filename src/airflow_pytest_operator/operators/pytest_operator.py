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
from typing import Any

from ..compat import BaseOperator
from ..exceptions import TestExecutionError, TestsFailedError
from ..reporters import JUnitResultParser, ResultParser
from ..runners import PytestRunner, SubprocessPytestRunner


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

    def __init__(
        self,
        *,
        test_path: str | Sequence[str],
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        fail_on_test_failure: bool = True,
        dry_run: bool = False,
        runner: PytestRunner | None = None,
        parser: ResultParser | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.test_path = test_path
        self.pytest_args = list(pytest_args) if pytest_args else []
        self.env = env or {}
        self.fail_on_test_failure = fail_on_test_failure
        self.dry_run = dry_run
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

        run_ok = False
        try:
            artifacts = self._runner.run(
                self.test_path,
                pytest_args=effective_args,
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
                self.log.error(
                    "Failed tests:\n  %s", "\n  ".join(result.failed_node_ids)
                )

            # The summary is returned from execute(); Airflow pushes it to
            # XCom under the standard "return_value" key when do_xcom_push is
            # on (the default). Pass do_xcom_push=False to disable. We push no
            # second custom key -- a single source of truth for the result.
            summary = result.to_xcom()

            run_ok = result.success
            if self.fail_on_test_failure and not result.success:
                raise TestsFailedError(result)

            return summary
        finally:
            # Always invoke cleanup; the runner decides what to remove
            # based on its policy and the success flag. Never let cleanup
            # errors mask the real outcome of execute().
            try:
                self._runner.cleanup(success=run_ok)
            except Exception:  # pragma: no cover - best-effort teardown
                self.log.exception("Error while cleaning up report directory")

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
