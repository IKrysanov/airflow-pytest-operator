"""The result-parser interface.

A parser turns a report file into a :class:`TestRunResult`. It knows
nothing about how the report was produced. Keeping this separate from
the runner means we can support other report formats (e.g. a JSON
report plugin) by adding a parser, not by editing existing code (OCP).
"""

# Copyright 2026 Ilya Krysanov
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

from ..models import TestRunResult


class ResultParser(ABC):
    """Parses a report file into a structured result."""

    @abstractmethod
    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        """Parse ``report_path`` into a :class:`TestRunResult`.

        ``exit_code`` is threaded through so the result records how the
        process actually terminated (a parser can't always infer e.g.
        an internal pytest error from the XML alone).

        Raises :class:`ReportParseError` if the file is missing or malformed.
        """
        raise NotImplementedError
