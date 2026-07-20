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

import logging
import os
import xml.etree.ElementTree as ET

try:  # prefer the hardened parser when present
    from defusedxml.ElementTree import parse as _xml_parse

    _HARDENED_XML = True
except ImportError:  # pragma: no cover - fallback path
    # Narrow on purpose: a defusedxml that is installed but broken must fail
    # loudly rather than silently downgrade us to the unhardened parser.
    from xml.etree.ElementTree import parse as _xml_parse

    _HARDENED_XML = False

from ..exceptions import ReportParseError
from ..models import CaseResult, ReportRequest, TestRunResult
from .base import ResultParser

_log = logging.getLogger(__name__)
_UNHARDENED_WARNED = False


def _warn_if_unhardened() -> None:
    """Warn once when parsing without defusedxml."""
    global _UNHARDENED_WARNED
    if _HARDENED_XML or _UNHARDENED_WARNED:
        return
    _UNHARDENED_WARNED = True
    _log.warning(
        "Parsing JUnit XML with the stdlib parser: defusedxml is not installed, "
        "so a malicious or corrupt report can exhaust worker memory via entity "
        "expansion. Install the extra: "
        "pip install 'airflow-pytest-operator[secure-xml]'."
    )


class JUnitResultParser(ResultParser):
    """Parse pytest's JUnit XML into a :class:`TestRunResult`.

    The output filename is fixed (``REPORT_FILENAME``) inside whatever
    directory the runner provides. If the same ``report_dir`` is reused
    across runs, each new pytest invocation overwrites the previous
    report -- there is no per-run uniquification at the parser level.
    Callers that need to retain historical reports should give the
    runner a fresh ``report_dir`` per run (the default temp-dir behavior
    does this automatically).

    Pass ``report_dir`` to place the report at a fixed location, e.g.
    ``JUnitResultParser(report_dir="/opt/airflow/artifacts")`` -- the
    operator forwards it to the default runner (see :class:`ResultParser`
    for the precedence rules).
    """

    REPORT_FILENAME = "junit.xml"

    def report_request(self, report_dir: str) -> ReportRequest:
        # The parser owns the report location: its own ``report_dir`` (set on
        # the constructor) wins; otherwise it falls back to the directory the
        # runner offers (a temp dir). The path is made absolute: the runner may
        # run pytest from a different cwd (it derives one from the test
        # targets), so a relative report path would be written somewhere other
        # than where the runner looks for it.
        path = os.path.abspath(
            os.path.join(self._report_dir or report_dir, self.REPORT_FILENAME)
        )
        return ReportRequest(
            pytest_args=(f"--junitxml={path}", "-o", "junit_logging=all"),
            report_path=path,
        )

    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        if not report_path or not os.path.exists(report_path):
            raise ReportParseError(f"JUnit report not found: {report_path!r}")

        _warn_if_unhardened()
        try:
            tree = _xml_parse(report_path)
        except (ET.ParseError, ValueError, OSError) as exc:
            raise ReportParseError(
                f"Failed to parse JUnit report {report_path!r}: {exc}"
            ) from exc

        root = tree.getroot()
        # The root may be <testsuites> (wrapping many) or a single <testsuite>.
        suites = list(root.iter("testsuite")) if root.tag == "testsuites" else [root]

        cases: list[CaseResult] = []
        for suite in suites:
            for tc in suite.findall("testcase"):
                cases.append(self._parse_case(tc))

        total = len(cases)
        passed = sum(1 for c in cases if c.outcome == "passed")
        failed = sum(1 for c in cases if c.outcome == "failed")
        errors = sum(1 for c in cases if c.outcome == "error")
        skipped = sum(1 for c in cases if c.outcome == "skipped")

        suite_durations = [
            t for t in (self._parse_time_attr(s) for s in suites) if t is not None
        ]
        if suite_durations:
            duration = sum(suite_durations)
        else:
            duration = sum(c.time for c in cases)

        return TestRunResult(
            total=total,
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            duration=round(duration, 4),
            exit_code=exit_code,
            cases=tuple(cases),
        )

    @staticmethod
    def _parse_time_attr(elem: ET.Element) -> float | None:
        """Parse an element's ``time`` attribute, or ``None`` if unusable.

        Returns ``None`` when the attribute is absent (a partial/truncated
        report) or non-numeric (a malformed report), letting the caller fall
        back to summing per-case times. A present, parseable value -- even
        ``0`` -- is returned as-is.
        """
        raw = elem.get("time")
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_case(tc: ET.Element) -> CaseResult:
        # JUnit XML has no native pytest node id. pytest's writer instead
        # splits it across two attributes: ``classname`` is the dotted
        # module(/class) path (e.g. ``tests.test_x`` or
        # ``tests.test_x.TestThings``) and ``name`` is the leaf test name,
        # with any parametrization preserved inline (e.g. ``test_y[1]``). The
        # JSON parser normalises its native slash-form nodeid into this exact
        # same shape, so both parsers yield identical ``CaseResult.node_id``
        # values. The dotted id is not a pytest CLI selector on its own; see
        # ``CaseResult.node_id`` and ``node_id_to_pytest_args`` for converting
        # it back to a runnable ``path/to/test.py::name`` for "retry failed".
        name = tc.get("name", "")
        classname = tc.get("classname", "")
        try:
            time = float(tc.get("time", "0") or 0)
        except ValueError:
            time = 0.0

        # Outcome precedence: error > failure > skipped > passed.
        failure = tc.find("failure")
        error = tc.find("error")
        skipped = tc.find("skipped")

        if error is not None:
            outcome, node = "error", error
        elif failure is not None:
            outcome, node = "failed", failure
        elif skipped is not None:
            outcome, node = "skipped", skipped
        else:
            outcome, node = "passed", None

        message = None
        if node is not None:
            message = node.get("message") or (node.text or "").strip() or None

        return CaseResult(
            name=name,
            classname=classname,
            time=time,
            outcome=outcome,
            message=message,
        )
