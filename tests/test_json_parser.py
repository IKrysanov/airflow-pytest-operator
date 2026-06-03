"""Parser tests against real pytest-json-report output.

Same approach as test_junit_parser.py: we run pytest on tiny throwaway
suites with the json-report plugin, then assert the parser interprets
the resulting JSON correctly. Catches drift in the plugin's schema far
better than hand-crafted JSON fixtures would.

These tests require the ``pytest-json-report`` plugin, which is part of
the package's ``[json-report]`` extra and is also pulled in by ``[dev]``.
If the plugin is missing the tests skip cleanly rather than failing.
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

import importlib.util
import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from airflow_pytest_operator.exceptions import ReportParseError
from airflow_pytest_operator.reporters import JSONResultParser

# Skip the entire module if the plugin isn't on the path. The parser itself
# does not require pytest-json-report to be importable (it just parses
# whatever JSON it is handed), but every test below needs a *real* report
# to assert against -- and we generate those by actually running pytest
# with the plugin. No plugin -> no real fixtures -> nothing meaningful to
# assert.
pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("pytest_jsonreport") is None,
    reason="pytest-json-report not installed; install via [json-report] or [dev] extra",
)


def _make_json(tmp_path: Path, suite_src: str) -> str:
    """Run pytest on a throwaway suite and return the JSON report path.

    Flags come from ``JSONResultParser().report_request(...)`` -- same
    principle as ``_make_junit`` in ``test_junit_parser.py``. Hardcoding
    them in two places is exactly the kind of drift 0.4.0's
    ``report_request`` abstraction is meant to prevent.
    """
    suite = tmp_path / "test_sample.py"
    suite.write_text(textwrap.dedent(suite_src))

    parser = JSONResultParser()
    spec = parser.report_request(str(tmp_path))

    subprocess.run(
        [sys.executable, "-m", "pytest", str(suite), *spec.pytest_args, "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert spec.report_path is not None
    report = Path(spec.report_path)
    assert report.exists(), "pytest did not produce a JSON report"
    return str(report)


def test_parses_mixed_outcomes(tmp_path):
    report = _make_json(
        tmp_path,
        """
        import pytest

        def test_pass(): assert True
        def test_fail(): assert 1 == 2
        @pytest.mark.skip(reason="nope")
        def test_skip(): pass
    """,
    )
    result = JSONResultParser().parse(report, exit_code=1)
    print(
        f"result: total={result.total}, passed={result.passed}, failed={result.failed}, skipped={result.skipped}, errors={result.errors}, success={result.success}"
    )
    print(f"failed_node_ids: {result.failed_node_ids}")

    assert result.total == 3
    assert result.passed == 1
    assert result.failed == 1
    assert result.skipped == 1
    assert result.errors == 0
    assert result.success is False
    assert result.exit_code == 1
    assert any("test_fail" in nid for nid in result.failed_node_ids)


def test_parses_all_passing(tmp_path):
    report = _make_json(
        tmp_path,
        """
        def test_a(): assert True
        def test_b(): assert True
    """,
    )
    result = JSONResultParser().parse(report, exit_code=0)
    assert result.success is True
    assert result.passed == 2
    assert result.failed_node_ids == []


def test_parses_errors_in_fixture(tmp_path):
    # An exception raised in setup shows up as "error" on the JSON side,
    # mirroring the JUnit dialect. Validates the outcome map covers it.
    report = _make_json(
        tmp_path,
        """
        import pytest

        @pytest.fixture
        def broken():
            raise RuntimeError("boom")

        def test_uses_broken(broken): pass
    """,
    )
    result = JSONResultParser().parse(report, exit_code=1)
    print(f"result: errors={result.errors}, success={result.success}")
    assert result.errors == 1
    assert result.success is False


def test_nodeid_round_trips_through_failed_node_ids(tmp_path):
    # The whole point of failed_node_ids is to be re-fed to pytest via -k or
    # as a positional arg. The plugin's "nodeid" is already in pytest's own
    # canonical form (file::test), so we store it verbatim and the value
    # must round-trip without losing the "::" structure.
    report = _make_json(
        tmp_path,
        """
        def test_x(): assert False
    """,
    )
    result = JSONResultParser().parse(report, exit_code=1)
    assert len(result.failed_node_ids) == 1
    nid = result.failed_node_ids[0]
    print(f"node_id: {nid!r}")
    assert "::test_x" in nid
    # Confirm it's not the synthesized "classname::name" form that JUnit
    # produces -- with no classname, CaseResult.node_id returns name as-is,
    # which means the JSON parser must put the full nodeid in `name`.
    assert nid.endswith("::test_x")


def test_xfail_is_treated_as_skipped(tmp_path):
    # @pytest.mark.xfail with a failing body produces outcome "xfailed",
    # which we fold into "skipped" so callers don't have to know about the
    # extra states (same shape as the JUnit parser).
    report = _make_json(
        tmp_path,
        """
        import pytest

        @pytest.mark.xfail
        def test_expected_failure(): assert False
    """,
    )
    result = JSONResultParser().parse(report, exit_code=0)
    assert result.skipped == 1
    assert result.failed == 0


def test_failure_message_is_captured(tmp_path):
    report = _make_json(
        tmp_path,
        """
        def test_fail():
            x = 1
            assert x == 999, "x was not 999"
    """,
    )
    result = JSONResultParser().parse(report, exit_code=1)
    failed = [c for c in result.cases if c.outcome == "failed"]
    assert len(failed) == 1
    print(f"failure message: {failed[0].message!r}")
    # longrepr includes the assertion text in some form. We don't pin the
    # exact format (it changes between pytest versions) -- only that we
    # surfaced *something* useful instead of leaving the message None.
    assert failed[0].message is not None
    assert failed[0].message.strip() != ""


def test_missing_report_raises():
    with pytest.raises(ReportParseError):
        JSONResultParser().parse("/nonexistent/report.json")


def test_malformed_json_raises(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{this is not valid json")
    with pytest.raises(ReportParseError):
        JSONResultParser().parse(str(bad))


def test_non_object_root_raises(tmp_path):
    # The schema requires the root to be an object. A bare list or scalar
    # is a sign someone handed us the wrong file (e.g. a JUnit XML report
    # they renamed, or a custom result format).
    bad = tmp_path / "bad.json"
    bad.write_text("[1, 2, 3]")
    with pytest.raises(ReportParseError):
        JSONResultParser().parse(str(bad))


def test_to_xcom_is_serializable(tmp_path):
    # Same XCom-safety check we apply to the JUnit parser. The result
    # dataclass is identical, so this mostly defends against future
    # JSON-side changes that might sneak a non-JSON-safe field in.
    report = _make_json(tmp_path, "def test_x(): assert True")
    result = JSONResultParser().parse(report)
    payload = result.to_xcom()
    print(f"xcom payload: {payload}")
    json.dumps(payload)
    assert "cases" not in payload
    assert payload["success"] is True


# ---------------------------------------------------------------------------
# report_request: the parser's declaration of what pytest must produce
# ---------------------------------------------------------------------------


def test_report_request_returns_expected_spec(tmp_path):
    spec = JSONResultParser().report_request(str(tmp_path))
    print(f"spec: report_path={spec.report_path!r}, pytest_args={spec.pytest_args}")

    expected_path = str(tmp_path / "report.json")
    assert spec.report_path == expected_path
    # The flags come in two tokens, not three -- pytest-json-report combines
    # the toggle and the path into a single --json-report-file=... value.
    assert spec.pytest_args == (
        "--json-report",
        f"--json-report-file={expected_path}",
    )


def test_report_request_uses_class_filename_constant(tmp_path):
    parser = JSONResultParser()
    spec = parser.report_request(str(tmp_path))
    assert spec.report_path is not None
    assert spec.report_path.endswith(parser.REPORT_FILENAME)


def test_report_request_composes_path_inside_given_dir(tmp_path):
    nested = tmp_path / "deep" / "nested"
    nested.mkdir(parents=True)
    spec = JSONResultParser().report_request(str(nested))
    assert spec.report_path is not None
    assert Path(spec.report_path).parent == nested


# ---------------------------------------------------------------------------
# Parity check with JUnit: the two built-in parsers are not bit-identical
# (collection-error semantics differ, durations are measured differently)
# but for a *clean* run on the same suite they must agree on the summary
# counts. This guards both parsers against silent drift.
# ---------------------------------------------------------------------------


def test_summary_matches_junit_for_passing_suite(tmp_path):
    from airflow_pytest_operator.reporters import JUnitResultParser

    suite_src = """
        import pytest

        def test_a(): assert True
        def test_b(): assert True
        def test_c(): assert 1 == 2
        @pytest.mark.skip(reason="nope")
        def test_d(): pass
    """
    suite = tmp_path / "test_pair.py"
    suite.write_text(textwrap.dedent(suite_src))

    # Same suite, both parsers, both reports produced in one pytest run.
    junit_spec = JUnitResultParser().report_request(str(tmp_path))
    json_spec = JSONResultParser().report_request(str(tmp_path))
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(suite),
            *junit_spec.pytest_args,
            *json_spec.pytest_args,
            "-q",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    junit = JUnitResultParser().parse(junit_spec.report_path, exit_code=1)
    json_result = JSONResultParser().parse(json_spec.report_path, exit_code=1)
    print(
        f"junit:  total={junit.total}, passed={junit.passed}, failed={junit.failed}, skipped={junit.skipped}, errors={junit.errors}"
    )
    print(
        f"json:   total={json_result.total}, passed={json_result.passed}, failed={json_result.failed}, skipped={json_result.skipped}, errors={json_result.errors}"
    )

    # Counts must agree; durations and exact failure messages can drift.
    assert junit.total == json_result.total == 4
    assert junit.passed == json_result.passed == 2
    assert junit.failed == json_result.failed == 1
    assert junit.skipped == json_result.skipped == 1
    assert junit.errors == json_result.errors == 0


# ---------------------------------------------------------------------------
# Defensive branches: malformed or partial reports. We can't easily provoke
# these via real pytest-json-report output, so they use hand-crafted JSON
# blobs. They protect the parser against schema drift and third-party tools
# that emit pytest-json-report-like documents with edge cases.
# ---------------------------------------------------------------------------


def _write_json(tmp_path: Path, payload: dict) -> str:
    p = tmp_path / "report.json"
    p.write_text(json.dumps(payload))
    return str(p)


def test_summary_falls_back_to_counted_cases_when_absent(tmp_path):
    # When "summary" is missing or empty, the parser counts the parsed cases
    # itself rather than reporting zero everything.
    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
            {"nodeid": "f.py::b", "outcome": "failed", "call": {"duration": 0.0}},
            {"nodeid": "f.py::c", "outcome": "skipped"},
            {"nodeid": "f.py::d", "outcome": "error"},
        ],
        # no "summary" key at all
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=1)
    assert result.total == 4
    assert result.passed == 1
    assert result.failed == 1
    assert result.skipped == 1
    assert result.errors == 1


def test_malformed_duration_degrades_to_zero(tmp_path):
    # A non-numeric top-level "duration" must not sink the parse -- mirrors
    # the JUnit parser's tolerance for a bad `time` attribute.
    payload = {
        "duration": "not-a-number",
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
        ],
        "summary": {"total": 1, "passed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    assert result.duration == 0.0
    assert result.passed == 1


def test_malformed_call_duration_degrades_to_zero(tmp_path):
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::a",
                "outcome": "passed",
                "call": {"duration": "boom"},
            },
        ],
        "summary": {"total": 1, "passed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    assert result.cases[0].time == 0.0


def test_non_dict_call_section_handled(tmp_path):
    # If "call" is somehow not a dict (e.g. None, or a stray string), the
    # per-case duration must default to 0 instead of raising AttributeError.
    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": None},
        ],
        "summary": {"total": 1, "passed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    assert result.cases[0].time == 0.0


def test_unknown_outcome_maps_to_skipped_with_warning(tmp_path, caplog):
    # An outcome we don't recognize (future plugin extension, custom hook)
    # is conservatively folded into "skipped" -- NOT "error" or "failed".
    # Reasoning: an unknown value most likely means pytest-json-report added
    # a new state; defaulting to "error" would flip previously-green runs to
    # red and raise TestsFailedError on the operator side. "skipped" is the
    # only bucket that's both honest ("we didn't count this as a real pass
    # or fail") and non-fatal. To stop drift from going silent, the parser
    # logs a WARNING the first time each unknown outcome is observed.
    import logging as _logging

    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "mystery", "call": {"duration": 0.0}},
            # A second test with the SAME unknown outcome must not produce
            # a second warning -- we dedupe per report.
            {"nodeid": "f.py::b", "outcome": "mystery", "call": {"duration": 0.0}},
            # A different unknown outcome IS reported alongside the first.
            {"nodeid": "f.py::c", "outcome": "weirder", "call": {"duration": 0.0}},
        ],
    }
    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)

    assert all(c.outcome == "skipped" for c in result.cases)
    # The run is not marked failed by an unknown outcome: this is the whole
    # point of the safer default.
    assert result.failed == 0
    assert result.errors == 0
    assert result.skipped == 3
    # One WARNING covers both distinct unknown values; the dedupe means the
    # log doesn't explode on a large drifted suite.
    warnings = [r for r in caplog.records if r.levelno == _logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "mystery" in msg and "weirder" in msg


def test_message_pulled_from_setup_when_call_missing(tmp_path):
    # A fixture error has longrepr on the "setup" phase, not "call". Verify
    # the call → setup → teardown probe order picks it up.
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::test_with_broken_fixture",
                "outcome": "error",
                "setup": {"duration": 0.0, "longrepr": "RuntimeError: broken"},
            },
        ],
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=1)
    assert result.cases[0].message == "RuntimeError: broken"


def test_message_pulled_from_crash_when_longrepr_absent(tmp_path):
    # Some pytest-json-report builds emit a structured "crash" object on
    # failures instead of a free-text "longrepr". The parser falls back to
    # crash.message so the failure isn't silent.
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::test_fail",
                "outcome": "failed",
                "call": {
                    "duration": 0.0,
                    "crash": {"message": "AssertionError: boom"},
                },
            },
        ],
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=1)
    assert result.cases[0].message == "AssertionError: boom"


def test_non_dict_test_entries_skipped(tmp_path):
    # If "tests" contains garbage (a string, a number), those entries are
    # quietly skipped rather than crashing the parse. The real cases are
    # still produced.
    payload = {
        "duration": 0.0,
        "tests": [
            "not a dict",
            42,
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
        ],
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    assert len(result.cases) == 1
    assert result.cases[0].name == "f.py::a"


def test_xpassed_counts_as_passed(tmp_path):
    # xpassed (unexpected pass when xfail was marked) is folded into the
    # "passed" bucket -- both in case-level mapping AND in the summary path
    # (where it lives under its own counter). Test exercises both.
    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "xpassed", "call": {"duration": 0.0}},
        ],
        "summary": {"total": 1, "xpassed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    assert result.passed == 1
    assert result.cases[0].outcome == "passed"


# ---------------------------------------------------------------------------
# Structural validation: malformed inputs should fail loudly with
# ReportParseError, not with random TypeError/AttributeError from deep
# inside the parser. These guard against drift in the document shape.
# ---------------------------------------------------------------------------


def test_non_list_tests_raises_report_parse_error(tmp_path):
    # If "tests" is not a list (scalar, dict, null), the parser must not
    # plough into a for-loop and crash with TypeError. The user gets a
    # single, catchable exception type for "this report is malformed".
    # Null is rejected too: pytest-json-report writes [] for a run with
    # zero tests, never null, so null is structurally suspect.
    for bad_value in ("not a list", 42, {"nope": True}, None):
        path = _write_json(tmp_path, {"tests": bad_value, "duration": 0.0})
        with pytest.raises(ReportParseError, match="non-list 'tests'"):
            JSONResultParser().parse(path)


def test_malformed_summary_value_warns_once(tmp_path, caplog):
    # A present-but-non-numeric value in a summary counter is a structural
    # mismatch with the schema. We coerce to 0 to keep going, but log a
    # WARNING so silent-zero counts don't disguise a malformed report.
    # Repeated bad keys deduplicate into a single warning line.
    import logging as _logging

    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
        ],
        "summary": {
            "total": 1,
            "passed": "yes please",  # bad
            "failed": "no thanks",  # bad
            "skipped": 0,
        },
    }
    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        result = JSONResultParser().parse(_write_json(tmp_path, payload))

    # Bad values silently coerced to 0 -- the run keeps going.
    assert result.passed == 0
    assert result.failed == 0
    # ...but one WARNING surfaced, listing every offending key.
    warnings = [r for r in caplog.records if r.levelno == _logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "passed" in msg
    assert "failed" in msg
    assert "non-numeric" in msg


def test_missing_summary_key_does_not_warn(tmp_path, caplog):
    # A *missing* counter is normal -- means "zero tests of that kind".
    # It must not trigger the malformed-summary warning, only a non-numeric
    # value should. This is the key invariant separating "partial report"
    # from "broken report".
    import logging as _logging

    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
        ],
        "summary": {"total": 1, "passed": 1},  # 'failed', 'error', 'skipped' absent
    }
    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        JSONResultParser().parse(_write_json(tmp_path, payload))
    warnings = [r for r in caplog.records if r.levelno == _logging.WARNING]
    assert warnings == []


# ---------------------------------------------------------------------------
# Skip-message extraction: pytest-json-report stores the skip reason as the
# repr of a 3-tuple (filename, lineno, 'Skipped: reason'). The parser must
# return the bare reason, not the tuple repr -- otherwise user-facing
# CaseResult.message reads like an error rather than a clean explanation.
# ---------------------------------------------------------------------------


def test_skipped_message_is_clean_reason_not_tuple(tmp_path):
    report = _make_json(
        tmp_path,
        """
        import pytest

        @pytest.mark.skip(reason="not relevant in this env")
        def test_skipme(): pass
        """,
    )
    result = JSONResultParser().parse(report, exit_code=0)
    skipped = [c for c in result.cases if c.outcome == "skipped"]
    assert len(skipped) == 1
    msg = skipped[0].message
    assert msg == "not relevant in this env"
    # Negative assertions: the tuple repr leakage we're guarding against.
    assert "(" not in msg
    assert "Skipped:" not in msg


def test_skipped_message_falls_back_on_schema_drift(tmp_path):
    # If pytest-json-report ever switches to a non-tuple longrepr (a plain
    # string), the parser must still return *something* useful rather
    # than swallowing the message. Fallback: hand back the text as-is.
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::test_x",
                "outcome": "skipped",
                "setup": {
                    "duration": 0.0,
                    "longrepr": "this is just a plain string, not a tuple repr",
                },
            },
        ],
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload))
    assert result.cases[0].message == "this is just a plain string, not a tuple repr"


def test_skipped_message_handles_unsafe_input(tmp_path):
    # ast.literal_eval refuses anything that isn't a Python literal -- if
    # the longrepr is structurally weird (e.g. someone hand-crafted a
    # report with code-looking text), we must NOT execute it and must NOT
    # crash. The fallback returns the raw text.
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::test_x",
                "outcome": "skipped",
                "setup": {
                    "duration": 0.0,
                    "longrepr": "__import__('os').system('echo pwned')",
                },
            },
        ],
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload))
    # Did not execute, did not crash, returned the literal text.
    assert "__import__" in result.cases[0].message


def test_skipped_via_pytest_skip_in_body(tmp_path):
    # When pytest.skip(...) is called from the test body (not setup),
    # the longrepr lands on "call". The fallback probe order must pick
    # it up so users still see a clean reason.
    report = _make_json(
        tmp_path,
        """
        import pytest

        def test_dyn_skip():
            pytest.skip("computed reason at runtime")
        """,
    )
    result = JSONResultParser().parse(report, exit_code=0)
    skipped = [c for c in result.cases if c.outcome == "skipped"]
    assert len(skipped) == 1
    assert skipped[0].message == "computed reason at runtime"


# ---------------------------------------------------------------------------
# xpassed under strict xfail: pytest-json-report classifies the unexpected
# pass as "failed" (not "xpassed") when strict=True. This means our
# summary-arithmetic (passed += summary["xpassed"]) does NOT double-count
# strict cases. This test pins that invariant so a future plugin change
# can't quietly break it.
# ---------------------------------------------------------------------------


def test_xpassed_strict_counts_as_failed_not_xpassed(tmp_path):
    report = _make_json(
        tmp_path,
        """
        import pytest

        @pytest.mark.xfail(strict=True)
        def test_actually_passes(): assert True
        """,
    )
    # exit_code=1 because strict-xpass is a failure as far as pytest is
    # concerned (it's the whole point of strict).
    result = JSONResultParser().parse(report, exit_code=1)

    # Strict xpass: counted as a failure, not a pass, not an xpassed.
    assert result.failed == 1
    assert result.passed == 0
    assert result.skipped == 0
    # If the plugin ever started ALSO incrementing "xpassed" for strict
    # cases, our summary-arithmetic would yield passed=1 -- catching this
    # in CI before it leaks to users.
    assert result.success is False
