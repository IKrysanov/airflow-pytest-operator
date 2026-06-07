"""The result-parser interface.

A parser turns a report file into a :class:`TestRunResult`. It knows
nothing about how the report was produced. Keeping this separate from
the runner means we can support other report formats (e.g. a JSON
report plugin) by adding a parser, not by editing existing code (OCP).
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

from ..models import ReportRequest, TestRunResult


class ResultParser(ABC):
    """Parses a report file into a structured result.

    A parser owns two responsibilities:

    * declare *what it needs* pytest to produce -- the CLI flags and the
      path where the report will land -- via :meth:`report_request`;
    * *interpret* that report into a :class:`TestRunResult` via
      :meth:`parse`.

    Together these let the operator stay format-agnostic: it asks the
    parser for a :class:`ReportRequest`, hands it to the runner, and
    feeds the resulting path back to the parser. Adding a new format
    (JSON, TAP, ...) is a new parser, not an edit of the runner.

    Report location
    ---------------
    A parser may carry the user's preferred *directory* for its report via
    the ``report_dir`` constructor argument. This is the user-facing place
    to say "put my report here" -- it lives on the parser because the parser
    already owns everything else about the report file (format, filename).
    The parser does not act on ``report_dir`` itself; the *operator* reads it
    and wires it into the default runner, which remains the owner of the
    directory lifecycle (temp-dir creation and cleanup). ``None`` means "no
    preference" -- the runner creates a temp directory and cleans it per its
    own ``cleanup`` policy.

    Precedence (resolved by the operator): an explicit ``report_dir`` on an
    *injected* runner wins; otherwise the parser's ``report_dir`` is applied
    to the default runner; otherwise a temp directory is used. If you inject
    your own runner, configure ``report_dir`` on that runner.
    """

    def __init__(self, *, report_dir: str | None = None) -> None:
        self._report_dir = report_dir

    @property
    def report_dir(self) -> str | None:
        """User-preferred directory for this parser's report, or ``None``.

        Read by the operator to configure the default runner; see the class
        docstring for the precedence rules.
        """
        return self._report_dir

    @abstractmethod
    def report_request(self, report_dir: str) -> ReportRequest:
        """Declare the pytest args and report path for this parser.

        ``report_dir`` is a directory the runner has prepared and into
        which the parser may place its report file. The implementation
        composes a path inside that directory (or returns ``None`` for
        ``report_path`` if it reads stdout) and the pytest CLI args that
        make pytest emit a report at that path.

        The returned :class:`ReportRequest` is opaque to the runner --
        it splices ``pytest_args`` verbatim and reports back whatever
        ``report_path`` was declared (or ``None`` if the file is missing
        after the run, e.g. on a collection error).
        """
        raise NotImplementedError

    @abstractmethod
    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        """Parse ``report_path`` into a :class:`TestRunResult`.

        ``exit_code`` is threaded through so the result records how the
        process actually terminated (a parser can't always infer e.g.
        an internal pytest error from the report alone).

        Raises :class:`ReportParseError` if the file is missing or malformed.
        """
        raise NotImplementedError
