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
    print(f"result: total={result.total}, passed={result.passed}, failed={result.failed}, skipped={result.skipped}, errors={result.errors}, success={result.success}")
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
    assert result.errors >= 1
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
    print(f"junit:  total={junit.total}, passed={junit.passed}, failed={junit.failed}, skipped={junit.skipped}, errors={junit.errors}")
    print(f"json:   total={json_result.total}, passed={json_result.passed}, failed={json_result.failed}, skipped={json_result.skipped}, errors={json_result.errors}")

    # Counts must agree; durations and exact failure messages can drift.
    assert junit.total == json_result.total == 4
    assert junit.passed == json_result.passed == 2
    assert junit.failed == json_result.failed == 1
    assert junit.skipped == json_result.skipped == 1
    assert junit.errors == json_result.errors == 0
