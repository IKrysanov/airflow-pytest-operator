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

"""The store interface for the ``failed_only`` cross-retry set."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class LastFailedStore(Protocol):
    """Carries a task instance's failed node-ids between Airflow retries.

    Structural (a ``Protocol``): any object with these three methods satisfies
    it -- no subclassing required -- so a custom backend type-checks cleanly
    when injected via ``PytestOperator(store=...)``. Implementations must be
    best-effort: never raise, and degrade to "no store" (an empty read, a
    skipped write/delete) so a bookkeeping problem cannot mask the test outcome.
    """

    def read(self, key: str) -> list[str]:
        """Return the stored failed node-ids for ``key``, or ``[]``."""
        ...

    def write(self, key: str, node_ids: list[str]) -> None:
        """Persist ``node_ids`` under ``key``."""
        ...

    def delete(self, key: str) -> None:
        """Delete ``key`` if present."""
        ...
