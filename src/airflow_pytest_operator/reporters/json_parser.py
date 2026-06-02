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

"""JSON result parser using pytest-json-report.

Parses the JSON document emitted by the ``pytest-json-report`` plugin
(``--json-report --json-report-file=...``). The plugin must be available
on the worker that runs the tests; install via the ``json-report`` extra::

    pip install airflow-pytest-operator[json-report]

If the plugin is missing, pytest itself exits with a usage error
("unrecognized arguments: --json-report"), the runner returns
``report_path=None``, and the operator surfaces the captured stderr via
``TestExecutionError`` -- the existing error path for "the run could not
produce a report". We deliberately do not probe for the plugin at parser
construction time: the parser lives in the operator's process while
pytest-json-report is needed wherever the runner spawns pytest, which
may be a different environment entirely (e.g. a Kubernetes pod started
by a custom runner). Validating in the wrong place would produce false
negatives.
"""

from __future__ import annotations

import json
import os
from typing import Any

from ..exceptions import ReportParseError
from ..models import CaseResult, ReportRequest, TestRunResult
from .base import ResultParser

# pytest-json-report outcomes map onto our four canonical states. xfail
# (expected failure, did fail) and xpass without strict (expected failure,
# unexpectedly passed) are treated as skipped/passed respectively, matching
# how pytest's JUnit dialect classifies them.
_OUTCOME_MAP = {
    "passed": "passed",
    "failed": "failed",
    "error": "error",
    "skipped": "skipped",
    "xfailed": "skipped",
    "xpassed": "passed",
}


class JSONResultParser(ResultParser):
    """Parse pytest-json-report output into a :class:`TestRunResult`."""

    REPORT_FILENAME = "report.json"

    def report_request(self, report_dir: str) -> ReportRequest:
        path = os.path.join(report_dir, self.REPORT_FILENAME)
        return ReportRequest(
            pytest_args=("--json-report", f"--json-report-file={path}"),
            report_path=path,
        )

    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        if not report_path or not os.path.exists(report_path):
            raise ReportParseError(f"JSON report not found: {report_path!r}")

        try:
            with open(report_path, encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            raise ReportParseError(
                f"Failed to parse JSON report {report_path!r}: {exc}"
            ) from exc

        if not isinstance(doc, dict):
            raise ReportParseError(
                f"JSON report {report_path!r} is not an object at the root"
            )

        cases: list[CaseResult] = []
        for raw in doc.get("tests", []):
            if isinstance(raw, dict):
                cases.append(self._parse_case(raw))

        # Prefer the report's own summary counters when present -- they
        # include collected-but-not-run cases that "tests[]" can miss --
        # and fall back to counting our parsed cases. This keeps the
        # numbers stable on partially-completed runs.
        summary = doc.get("summary") or {}
        if isinstance(summary, dict) and summary:
            total = _coerce_int(summary.get("total"), default=len(cases))
            passed = _coerce_int(summary.get("passed"))
            failed = _coerce_int(summary.get("failed"))
            errors = _coerce_int(summary.get("error"))
            skipped = _coerce_int(summary.get("skipped")) + _coerce_int(
                summary.get("xfailed")
            )
            # xpassed counts as passed in our mapping; reflect it here too.
            passed += _coerce_int(summary.get("xpassed"))
        else:
            total = len(cases)
            passed = sum(1 for c in cases if c.outcome == "passed")
            failed = sum(1 for c in cases if c.outcome == "failed")
            errors = sum(1 for c in cases if c.outcome == "error")
            skipped = sum(1 for c in cases if c.outcome == "skipped")

        # Top-level "duration" is the whole pytest run including
        # collection -- more accurate than summing per-test durations,
        # which omit collection time entirely.
        try:
            duration = float(doc.get("duration", 0.0) or 0.0)
        except (TypeError, ValueError):
            duration = 0.0

        return TestRunResult(
            total=total,
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            duration=round(duration, 4),
            exit_code=exit_code,
            cases=cases,
        )

    @staticmethod
    def _parse_case(raw: dict[str, Any]) -> CaseResult:
        nodeid = str(raw.get("nodeid", "") or "")
        outcome_raw = str(raw.get("outcome", "") or "")
        outcome = _OUTCOME_MAP.get(outcome_raw, "error")

        call = raw.get("call") or {}
        try:
            time = (
                float(call.get("duration", 0.0) or 0.0)
                if isinstance(call, dict)
                else 0.0
            )
        except (TypeError, ValueError):
            time = 0.0

        message = _extract_message(raw, outcome)

        # The nodeid already carries the full pytest-style identifier
        # ("tests/test_x.py::test_y"). We put it in `name` and leave
        # `classname` empty so CaseResult.node_id returns the nodeid
        # verbatim via the no-classname fallback.
        return CaseResult(
            name=nodeid,
            classname="",
            time=time,
            outcome=outcome,
            message=message,
        )


def _coerce_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _extract_message(raw: dict[str, Any], outcome: str) -> str | None:
    """Pull a short failure/error/skip message out of the test record.

    pytest-json-report stores diagnostic text in ``longrepr`` on whichever
    phase failed -- ``call`` for test-body failures, ``setup`` for fixture
    errors and skips, ``teardown`` for teardown errors. We probe call →
    setup → teardown in that order. ``passed`` cases never carry a message.
    """
    if outcome == "passed":
        return None
    for phase in ("call", "setup", "teardown"):
        section = raw.get(phase)
        if not isinstance(section, dict):
            continue
        longrepr = section.get("longrepr")
        if isinstance(longrepr, str) and longrepr.strip():
            return longrepr.strip()
        crash = section.get("crash")
        if isinstance(crash, dict):
            msg = crash.get("message")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
    return None
