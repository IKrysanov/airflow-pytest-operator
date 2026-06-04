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
import logging
import os
from typing import Any

from ..exceptions import ReportParseError
from ..models import CaseResult, ReportRequest, TestRunResult
from .base import ResultParser

_log = logging.getLogger(__name__)

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

# Default for outcomes we don't recognize. Deliberately "skipped", not
# "error": if a future pytest-json-report adds a new state (e.g. "deselected",
# "warned"), classifying it as an error would flip a clean run to failed and
# raise TestsFailedError on previously-green suites. "skipped" is the only
# bucket that's both honest ("we didn't count this as a real pass or fail")
# and non-fatal. Drift doesn't go silent though -- we log a WARNING the
# first time each unknown outcome appears, so a real schema change still
# shows up in worker logs rather than being papered over forever.
_UNKNOWN_OUTCOME_FALLBACK = "skipped"


class JSONResultParser(ResultParser):
    """Parse pytest-json-report output into a :class:`TestRunResult`.

    The output filename is fixed (``REPORT_FILENAME``) inside whatever
    directory the runner provides. If the same ``report_dir`` is reused
    across runs, each new pytest invocation overwrites the previous
    report -- there is no per-run uniquification at the parser level.
    Callers that need to retain historical reports should give the
    runner a fresh ``report_dir`` per run (the default temp-dir behavior
    does this automatically).
    """

    REPORT_FILENAME = "report.json"

    def report_request(self, report_dir: str) -> ReportRequest:
        path = os.path.join(report_dir, self.REPORT_FILENAME)
        return ReportRequest(
            pytest_args=("--json-report", f"--json-report-file={path}"),
            report_path=path,
        )

    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        """Parse a pytest-json-report document into a :class:`TestRunResult`.

        Counter sourcing: when the document has a ``summary`` block (the
        common case), ``total``/``passed``/``failed``/``skipped``/``errors``
        come from there. Otherwise we count the entries we parsed out of
        ``tests[]``. The ``cases`` list always reflects only the entries
        in ``tests[]``.

        These two can disagree -- typically when the run was interrupted
        (early exit, ``--collect-only``, internal crash): the summary
        records the collected total while ``tests[]`` only lists the cases
        that actually ran. The summary stays authoritative for counts
        because it reflects intent; ``cases`` stays authoritative for
        per-case detail because that's all we have. The upshot is that
        ``len(result.cases) == result.total`` is **not** an invariant
        callers may rely on -- compare against ``result.passed +
        result.failed + result.skipped + result.errors`` instead, or use
        ``failed_node_ids`` (which is always derived from ``cases``).
        """

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

        # Track unknown outcomes seen in this report so we can WARN once per
        # distinct value instead of spamming the log for every test in a big
        # suite that drifted.
        unknown_outcomes_seen: set[str] = set()

        raw_tests = doc.get("tests", [])
        if not isinstance(raw_tests, list):
            raise ReportParseError(
                f"JSON report {report_path!r} has non-list 'tests' field "
                f"(got {type(raw_tests).__name__})"
            )

        cases: list[CaseResult] = []
        for raw in raw_tests:
            if isinstance(raw, dict):
                cases.append(self._parse_case(raw, unknown_outcomes_seen))

        if unknown_outcomes_seen:
            _log.warning(
                "JSON report %r contained outcome(s) not in the parser's "
                "mapping (%s); each was treated as %r. This usually means "
                "pytest-json-report added a new outcome value -- please file "
                "an issue against airflow-pytest-operator so the mapping is "
                "updated.",
                report_path,
                ", ".join(sorted(unknown_outcomes_seen)),
                _UNKNOWN_OUTCOME_FALLBACK,
            )

        # Prefer the report's own summary counters when present -- they
        # include collected-but-not-run cases that "tests[]" can miss --
        # and fall back to counting our parsed cases. This keeps the
        # numbers stable on partially-completed runs.
        bad_summary_keys: list[str] = []

        def coerce(key: str, *, default: int = 0) -> int:
            return _coerce_int(
                summary.get(key),
                default=default,
                _bad=bad_summary_keys,
                _key=key,
            )

        summary = doc.get("summary") or {}
        if isinstance(summary, dict) and summary:
            total = coerce("total", default=len(cases))
            passed = coerce("passed")
            failed = coerce("failed")
            errors = coerce("error")  # pytest-json-report uses singular
            skipped = coerce("skipped") + coerce("xfailed")
            # xpassed counts as passed in our mapping. NB: with strict
            # xfail, pytest-json-report classifies the unexpected pass as
            # "failed" instead and removes it from xpassed -- so there is
            # no double-count to worry about. See test_xpassed_strict.
            passed += coerce("xpassed")
        else:
            total = len(cases)
            passed = sum(1 for c in cases if c.outcome == "passed")
            failed = sum(1 for c in cases if c.outcome == "failed")
            errors = sum(1 for c in cases if c.outcome == "error")
            skipped = sum(1 for c in cases if c.outcome == "skipped")

        if bad_summary_keys:
            _log.warning(
                "JSON report %r had non-numeric value(s) in summary keys "
                "(%s); each was treated as 0. The report is likely "
                "malformed or from an incompatible plugin version.",
                report_path,
                ", ".join(sorted(set(bad_summary_keys))),
            )

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
    def _parse_case(
        raw: dict[str, Any],
        unknown_outcomes_seen: set[str],
    ) -> CaseResult:
        nodeid = str(raw.get("nodeid", "") or "")
        outcome_raw = str(raw.get("outcome", "") or "")
        outcome = _OUTCOME_MAP.get(outcome_raw)
        if outcome is None:
            # Unknown outcome -- record it for the one-shot warning above
            # and fall back to a non-fatal classification. See the comment
            # on _UNKNOWN_OUTCOME_FALLBACK for the rationale.
            unknown_outcomes_seen.add(outcome_raw)
            outcome = _UNKNOWN_OUTCOME_FALLBACK

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


def _coerce_int(
    value: Any,
    *,
    default: int = 0,
    _bad: list[str] | None = None,
    _key: str | None = None,
) -> int:
    """Best-effort int coercion with optional drift-tracking.

    A missing key (``value is None``) is **not** drift -- a partial summary
    is normal (zero tests of a kind ran). A *present-but-non-numeric* value
    IS drift: it means the report's shape is wrong. When ``_bad`` and
    ``_key`` are supplied, that case records the key into ``_bad`` so the
    caller can WARN once with the full list rather than silently zero out
    structural errors.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        if _bad is not None and _key is not None:
            _bad.append(_key)
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

    if outcome == "skipped":
        return _extract_skip_reason(raw)

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


def _extract_skip_reason(raw: dict[str, Any]) -> str | None:
    """Pull the clean skip reason out of a pytest-json-report skipped entry.

    The plugin records skips with longrepr set to the *repr* (not JSON
    serialization) of a 3-tuple ``(filename, lineno, 'Skipped: reason')``.
    We try to recover just the reason; on any mismatch we fall back to
    returning the raw longrepr so callers at least see something rather
    than ``None`` -- this keeps the parser tolerant to schema drift
    (the plugin may eventually switch to a structured object).
    """

    for phase in ("setup", "call"):
        section = raw.get(phase)
        if not isinstance(section, dict):
            continue
        longrepr = section.get("longrepr")
        if not isinstance(longrepr, str) or not longrepr.strip():
            continue

        text = longrepr.strip()

        # Try the structured shape first. ast.literal_eval safely parses
        # the repr of a tuple without executing arbitrary code (unlike
        # eval()), and refuses anything that isn't a Python literal.
        try:
            import ast

            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError, MemoryError):
            parsed = None

        if isinstance(parsed, tuple) and len(parsed) == 3:
            reason = parsed[2]
            if isinstance(reason, str):
                # Strip the conventional "Skipped: " prefix the plugin
                # bakes in so users get just the reason they wrote.
                if reason.startswith("Skipped: "):
                    reason = reason[len("Skipped: ") :]
                return reason.strip() or None

        return text

    return None
