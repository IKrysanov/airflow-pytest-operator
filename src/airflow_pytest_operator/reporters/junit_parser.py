"""JUnit XML result parser.

Parses the JUnit XML that pytest emits via ``--junitxml``. We use the
stdlib ``xml.etree`` with ``defusedxml`` when available to avoid XML
attack vectors on untrusted reports.
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

import os
import xml.etree.ElementTree as ET

try:  # prefer the hardened parser when present
    from defusedxml.ElementTree import parse as _xml_parse
except Exception:  # pragma: no cover - fallback path
    from xml.etree.ElementTree import parse as _xml_parse

from ..exceptions import ReportParseError
from ..models import CaseResult, ReportRequest, TestRunResult
from .base import ResultParser


class JUnitResultParser(ResultParser):
    """Parse pytest's JUnit XML into a :class:`TestRunResult`.

    The output filename is fixed (``REPORT_FILENAME``) inside whatever
    directory the runner provides. If the same ``report_dir`` is reused
    across runs, each new pytest invocation overwrites the previous
    report -- there is no per-run uniquification at the parser level.
    Callers that need to retain historical reports should give the
    runner a fresh ``report_dir`` per run (the default temp-dir behavior
    does this automatically).
    """

    REPORT_FILENAME = "junit.xml"

    def report_request(self, report_dir: str) -> ReportRequest:
        path = os.path.join(report_dir, self.REPORT_FILENAME)
        return ReportRequest(
            pytest_args=(f"--junitxml={path}", "-o", "junit_logging=all"),
            report_path=path,
        )

    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        if not report_path or not os.path.exists(report_path):
            raise ReportParseError(f"JUnit report not found: {report_path!r}")

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
    def _parse_case(tc: ET.Element) -> CaseResult:
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
