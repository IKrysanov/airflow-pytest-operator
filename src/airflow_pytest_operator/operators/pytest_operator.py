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
from ._constants import (
    COLLECT_ONLY_ALIASES,
    DIST_FLAGS,
    DIST_MODES,
    KEYWORD_FLAGS,
    MARKER_FLAGS,
    MAX_STDERR_LEN,
    NUMPROCESSES_FLAGS,
    PARALLEL_KEYWORDS,
    RETRY_STRATEGIES,
    has_flag,
)


class PytestOperator(BaseOperator):
    """Run a pytest suite as an Airflow task.

    Orchestrates an injectable runner -> parser and handles the Airflow side
    (templating, XCom, fail policy, retries). The structured summary returned
    by ``execute`` is pushed to XCom under ``return_value`` unless Airflow's
    ``do_xcom_push=False`` is set. See the README for full behaviour and
    examples.

    :param test_path: pytest target(s) -- a file, directory, or node-id, or a
        sequence of them (templated).
    :param pytest_args: extra pytest CLI args, e.g. ``["-x", "-q"]`` (templated).
    :param markers: ``-m`` marker expression, e.g. ``"smoke and not slow"``
        (templated). Sugar over ``pytest_args``; defers to an explicit ``-m``
        there and is skipped if it renders empty. Default None.
    :param keyword: ``-k`` keyword expression, e.g. ``"login or logout"``
        (templated). Same rules as ``markers``. Default None.
    :param env: extra environment variables for the run (templated).
    :param env_file: path to a ``.env`` merged into the test subprocess
        (templated). The operator only forwards the path; the runner reads and
        merges it with precedence ``os.environ`` < ``env_file`` < ``env``.
        ``AIRFLOW*`` keys are skipped unless ``env_file_overrides=True``. Needs
        the ``[dotenv]`` extra; a missing file/dependency fails the run.
        Default None.
    :param env_file_overrides: let ``env_file`` set ``AIRFLOW*`` keys. Default
        False (the explicit ``env`` is never restricted).
    :param fail_on_test_failure: fail the task on any test failure/error. If
        False, the task always succeeds and the outcome lives only in XCom.
        Default True.
    :param dry_run: invoke pytest with ``--collect-only`` -- import and collect,
        but run no test bodies (module-level code still runs). A collection
        error still fails the task. Useful as a DAG pre-flight. Default False.
    :param test_retry_strategy: how Airflow *retries* re-run the suite. ``"all"``
        (default) re-runs everything; ``"failed_only"`` re-runs only the previous
        attempt's failures, carried across retries in an Airflow Variable
        (consumed on read, written only when a further retry will read it).
        Best-effort: falls back to the full suite if unavailable; ignored in
        ``dry_run``.
    :param rerun_failed: extra **in-process** rounds re-running only the failed
        tests within one task execution (no pytest cache, no Airflow retry).
        ``0`` (default) disables it. On rerun the XCom summary adds
        ``rerun_rounds``, ``recovered_node_ids`` and ``still_failing_node_ids``;
        the task fails only if tests still fail after all rounds. Ignored in
        ``dry_run``. Non-negative int.
    :param parallel: pytest-xdist worker count -- an int, or ``"auto"`` /
        ``"logical"``; maps to ``-n``. Applied to the first full run only (not
        the ``rerun_failed`` rounds), ignored in ``dry_run``, and deferred to an
        explicit ``-n`` in ``pytest_args``. Needs the ``[xdist]`` extra on the
        worker. Default None (serial).
    :param dist: pytest-xdist ``--dist`` mode -- ``"load"`` (default; spread
        tests), ``"loadscope"`` / ``"loadfile"`` / ``"loadgroup"`` (pin a
        module-or-class / file / group to one worker), ``"worksteal"``,
        ``"each"``, ``"no"``. Requires ``parallel``. Default None.
    :param store: injectable ``LastFailedStore`` for the ``failed_only`` set
        (default: ``VariableLastFailedStore``). Any object with
        ``read``/``write``/``delete`` works (structural typing). Unused unless
        ``test_retry_strategy="failed_only"``.
    :param runner: injectable :class:`PytestRunner` (default: subprocess).
    :param parser: injectable :class:`ResultParser` (default: JUnit); it owns
        the report location via its ``report_dir``.
    """

    # Airflow Jinja-templates these attributes before execute() runs. The
    # internal option vocabulary and the ``has_flag`` helper live in
    # ``_constants.py`` to keep this module focused on orchestration.
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
        runner: PytestRunner | None = None,
        parser: ResultParser | None = None,
        store: LastFailedStore | None = None,
        **kwargs: Any,
    ) -> None:
        # Convention throughout: wrong *type* -> TypeError, valid type/wrong
        # *value* -> ValueError.
        if test_retry_strategy not in RETRY_STRATEGIES:
            raise ValueError(
                "test_retry_strategy must be one of 'all', 'failed_only'; "
                f"got {test_retry_strategy!r}"
            )
        # markers/keyword: only the type is checked here; an empty/blank value
        # is left to execute() (a template may render to "" and be skipped).
        for _name, _value in (("markers", markers), ("keyword", keyword)):
            if _value is not None and not isinstance(_value, str):
                raise TypeError(
                    f"{_name} must be a str (a pytest -m/-k expression) or None; "
                    f"got {type(_value).__name__}"
                )
        # bool is an int subclass -- reject it explicitly (it's not a count).
        if isinstance(rerun_failed, bool) or not isinstance(rerun_failed, int):
            raise TypeError(
                "rerun_failed must be an int (not bool); "
                f"got {type(rerun_failed).__name__}"
            )
        if rerun_failed < 0:
            raise ValueError(
                f"rerun_failed must be a non-negative integer; got {rerun_failed!r}"
            )
        # parallel: xdist worker count. None = serial; int must be >= 1;
        # "auto"/"logical" are xdist keywords. bool rejected explicitly.
        if parallel is not None:
            if isinstance(parallel, bool):
                raise TypeError(
                    "parallel must be an int or 'auto'/'logical' (not bool); "
                    f"got {type(parallel).__name__}"
                )
            if isinstance(parallel, int):
                if parallel < 1:
                    raise ValueError(
                        f"parallel must be >= 1 (or None to disable); got {parallel!r}"
                    )
            elif isinstance(parallel, str):
                if parallel not in PARALLEL_KEYWORDS:
                    raise ValueError(
                        "parallel string must be one of 'auto', 'logical'; "
                        f"got {parallel!r}"
                    )
            else:
                raise TypeError(
                    "parallel must be an int, 'auto'/'logical', or None; "
                    f"got {type(parallel).__name__}"
                )
        # dist: xdist scheduler mode. Require parallel -- --dist is inert
        # without -n, so reject it alone rather than silently no-op.
        if dist is not None:
            if dist not in DIST_MODES:
                raise ValueError(
                    f"dist must be one of {', '.join(sorted(DIST_MODES))}; got {dist!r}"
                )
            if parallel is None:
                raise ValueError(
                    "dist requires parallel to be set (a worker count or "
                    "'auto'/'logical'); --dist has no effect without -n."
                )
        # Fail fast on a bad store: the runtime_checkable protocol rejects
        # anything missing read/write/delete (structural -- methods only).
        if store is not None and not isinstance(store, LastFailedStore):
            raise TypeError(
                "store must implement the LastFailedStore protocol -- an object "
                "with read(key), write(key, ids) and delete(key) methods, e.g. "
                "the default VariableLastFailedStore(). "
                f"Got {type(store).__name__}."
            )
        # env keys/values become child env vars and must be strings: a non-str
        # (e.g. a bare True) otherwise fails deep in os.fsencode. Reject here,
        # naming the offending key.
        if env is not None:
            if not isinstance(env, dict):
                raise TypeError(
                    f"env must be a dict[str, str] or None; got {type(env).__name__}"
                )
            for key, value in env.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"env keys must be str (env vars are strings); "
                        f"got {type(key).__name__} ({key!r})"
                    )
                if not isinstance(value, str):
                    raise TypeError(
                        f"env[{key!r}] must be a str (env vars are strings); "
                        f"got {type(value).__name__}"
                    )
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

        # markers / keyword: sugar for pytest's -m / -k, spliced into the first
        # full run (and dry-run collection). An empty/blank value is skipped;
        # defer to the same flag already in pytest_args. Not applied to the
        # rerun_failed rounds (they target explicit node-ids).
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

        # pytest-xdist: -n / --dist on the first full run only (rerun_failed
        # rounds stay serial; skipped in dry-run). If the user already drives
        # -n/--numprocesses, they own parallelism -- defer and skip --dist too.
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
                # Skip --dist if the user already set it: deference is keyed on
                # -n above, so a lone --dist would otherwise be duplicated
                # (xdist keeps the last, dropping theirs).
                if self.dist is not None:
                    if has_flag(effective_args, DIST_FLAGS):
                        self.log.warning(
                            "dist=%r ignored: pytest_args already sets --dist; "
                            "deferring to your explicit arg.",
                            self.dist,
                        )
                    else:
                        effective_args += ["--dist", self.dist]

        # failed_only: narrow to the tests that failed on the previous attempt,
        # carried across Airflow retries in a Variable. We narrow whenever the
        # store holds a set (not gated on try_number), else run the full suite.
        # Consume-on-read: delete the Variable as soon as we read it, before the
        # (possibly crashing) run, so a dead worker can't orphan it; a fresh copy
        # is written at the end only if a retry will read it. No-op in dry-run.
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
            # First pass: snapshot its summary right away -- that's the honest
            # picture pushed to XCom even if the reruns below recover failures.
            # ``result`` tracks the latest run; ``summary`` holds the snapshot.
            result = self._run_and_parse(targets, effective_args)
            summary = dict(result.to_xcom())
            still_failing = list(result.failed_node_ids)
            rerun_rounds = 0

            # In-process reruns of ONLY the failed tests -- no pytest cache, no
            # Airflow retry, so robust on any executor. Skipped in dry-run and
            # when nothing failed.
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
                    result = self._run_and_parse(selectors, list(self.pytest_args))
                    still_failing = list(result.failed_node_ids)

            # Only when reruns happened, add the post-rerun view to the snapshot
            # so the final outcome is unambiguous.
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

            # failed_only: hand the still-failing set to the next attempt, but
            # ONLY when one will read it -- this attempt failed, fails the task
            # (fail_on_test_failure), and isn't the final attempt. Otherwise we
            # write nothing, so no terminal attempt orphans a Variable. Written
            # before the raise below so the failing attempt hands failures
            # forward; is_final_attempt gets self.log to surface its warning.
            if (
                var_key is not None
                and still_failing
                and self.fail_on_test_failure
                and not is_final_attempt(context, log=self.log)
            ):
                self._safe_store_write(var_key, still_failing)

            if self.fail_on_test_failure and not run_ok:
                raise TestsFailedError(result)

            return summary
        finally:
            # Always clean up; the runner decides what to remove from its policy
            # and the success flag. Wrapped so a cleanup error never masks the
            # real outcome. The failed_only Variable is untouched here on purpose
            # (consumed on read, written only for a retry that reads it).
            self._safe_cleanup(success=run_ok)

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
            # env_file forwarded as a path; the runner reads/merges it (the
            # operator does no filesystem work).
            env_file=self.env_file,
            env_file_overrides=self.env_file_overrides,
            # The parser decides the report flags/location; the runner just
            # splices them -- which keeps the runner format-agnostic.
            report_request=self._parser.report_request,
        )

        # Surface child output in the task log regardless of outcome.
        if artifacts.stdout:
            self.log.info("pytest stdout:\n%s", artifacts.stdout)
        if artifacts.stderr:
            self.log.warning("pytest stderr:\n%s", artifacts.stderr)

        # No report -> pytest never wrote one (collection error, crash, OOM,
        # wrong path, missing report-plugin). An execution failure, not a test
        # failure -- surface it with the captured stderr.
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
        return result

    # -- best-effort collaborator calls ---------------------------------
    #
    # cleanup/read/delete/write are best-effort by contract. Since runner/store
    # are injection points, a custom one could still raise; these wrappers keep a
    # bookkeeping/teardown error from masking execute()'s real outcome.

    def _safe_cleanup(self, *, success: bool) -> None:
        """Invoke the runner's cleanup; teardown must never mask the outcome.

        Used both between in-process reruns (to free each round's report dir)
        and in ``execute``'s ``finally``.
        """
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
        """Hand the failed set forward; a store error must not mask the result.

        This write sits right before the ``TestsFailedError`` raise, so an
        exception here from a contract-violating store would otherwise replace
        the genuine test-failure signal with a bookkeeping traceback.
        """
        try:
            self._store.write(key, node_ids)
        except Exception:
            self.log.exception("Error while writing failed_only Variable %r", key)

    def on_kill(self) -> None:
        """Abort the test run when Airflow terminates the task.

        Called on timeout, manual clear/mark-failed, or worker SIGTERM. We
        delegate to the runner, which owns the spawned process/resources; the
        operator knows nothing about subprocesses. Runners with nothing to
        cancel inherit a safe no-op.
        """
        self.log.warning("Task killed -- cancelling pytest run on %s", self.test_path)
        try:
            self._runner.cancel()
        except Exception:  # pragma: no cover - best-effort teardown
            # on_kill must never raise -- it runs during teardown.
            self.log.exception("Error while cancelling pytest run")

        # cancel() stopped the process, but the "always" policy still removes
        # the temp report dir -- kills/timeouts are exactly when dirs leak, so
        # clean here too. cleanup() is idempotent, so racing execute()'s finally
        # is harmless.
        try:
            self._runner.cleanup(success=False)
        except Exception:  # pragma: no cover - best-effort teardown
            self.log.exception("Error while cleaning up report directory")
