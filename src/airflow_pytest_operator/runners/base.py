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

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence

from ..models import OutputSink, ReportRequest, RunArtifacts


class PytestRunner(ABC):
    """Executes pytest against a target path and returns artifacts."""

    @abstractmethod
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
        """Run pytest and return where to find its outputs.

        ``test_path`` is a single target (file, directory, or node-id
        selector) or a sequence of such targets -- all passed to pytest as
        positional arguments. An empty sequence is rejected.

        ``env_file`` is an optional path to a ``.env`` file the runner should
        merge into the child environment (precedence ``os.environ`` <
        ``env_file`` < ``env``); ``env_file_overrides`` controls whether the
        file may override ``AIRFLOW*`` keys. Both are keyword-only with defaults,
        so a runner that does not support ``.env`` loading can ignore them -- but
        one handed a non-``None`` ``env_file`` it cannot honour should raise
        rather than silently drop it. The operator forwards both from its own
        ``env_file`` / ``env_file_overrides`` parameters.

        Implementations MUST:
          * always set ``RunArtifacts.report_path`` to the parser-declared
            path on success, or ``None`` if the run could not produce it,
          * never raise on *test* failure -- a failing test is a valid
            outcome reflected in ``exit_code``,
          * raise :class:`TestExecutionError` only when pytest itself
            could not be launched.

        ``report_request`` is keyword-only and required -- there is no
        sensible default ("just run pytest with no report" produces
        artifacts no parser can consume).

        ``on_output`` is an optional sink for *live* child output: when given,
        the runner calls it once per line as the line is drained (newline
        stripped), as ``on_output(line, stream)`` with ``stream`` being
        ``"stdout"`` or ``"stderr"``. It lets the operator stream pytest output
        to the task log in real time instead of one blob at the end. Keyword-only
        with a ``None`` default, so a runner that does not support streaming may
        ignore it (the full output is still returned in ``RunArtifacts`` either
        way). The same output cap applies to streamed lines.
        """
        raise NotImplementedError

    def cancel(self) -> None:
        """Abort an in-progress run, if one is active.

        Called by the operator's ``on_kill`` when Airflow terminates the
        task (timeout, manual clear, worker shutdown). Implementations
        that own external resources (a child process, a container, a pod)
        MUST terminate them here to avoid orphaned work on the worker.

        This is intentionally **not** abstract: a default no-op keeps the
        contract substitutable (Liskov) for runners that have nothing to
        cancel -- e.g. a synchronous in-process runner. Calling ``cancel``
        when no run is active, or calling it twice, MUST be safe.
        """
        return None

    def cleanup(self, *, success: bool = True) -> None:
        """Release any temporary resources created for the last run.

        Called by the operator after results have been parsed. The
        ``success`` flag lets implementations keep artifacts on failure
        for post-mortem analysis. Default is a no-op so runners that
        produce nothing to clean stay substitutable (Liskov). Must be
        safe to call when there is nothing to clean.
        """
        return None
