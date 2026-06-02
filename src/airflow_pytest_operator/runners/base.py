"""The Runner interface.

A Runner is responsible for *executing* a pytest run and producing
:class:`RunArtifacts`. It is NOT responsible for interpreting results.
That single responsibility is what makes runners interchangeable
(Liskov): the operator depends only on this abstraction (DIP), so a
``DockerPytestRunner`` or ``KubernetesPodPytestRunner`` could be dropped
in later without changing the operator.
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

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence

from ..models import ReportRequest, RunArtifacts


class PytestRunner(ABC):
    """Executes pytest against a target path and returns artifacts."""

    @abstractmethod
    def run(
        self,
        test_path: str,
        *,
        pytest_args: Sequence[str] | None = None,
        env: dict[str, str] | None = None,
        report_request: Callable[[str], ReportRequest],
    ) -> RunArtifacts:
        """Run pytest and return where to find its outputs.

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
