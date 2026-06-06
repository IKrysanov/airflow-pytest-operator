"""Parser tests against real pytest-generated JUnit XML.

We generate genuine reports by running pytest on tiny throwaway suites,
then assert the parser interprets them correctly. This catches drift in
pytest's XML dialect far better than hand-written fixtures.
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

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from airflow_pytest_operator.exceptions import ReportParseError
from airflow_pytest_operator.reporters import JUnitResultParser


def _make_junit(tmp_path: Path, suite_src: str) -> str:
    """Run pytest on a throwaway suite and return the JUnit report path.

    Flags come from ``JUnitResultParser().report_request(...)`` rather than
    being hardcoded here. This is deliberate: hardcoding them in two places
    is exactly the bug 0.4.0 is meant to prevent (runner had its own copy
    of ``--junitxml`` / ``junit_logging=all``). If the parser ever changes
    which flags it requests, these tests follow automatically.
    """
    suite = tmp_path / "test_sample.py"
    suite.write_text(textwrap.dedent(suite_src))

    parser = JUnitResultParser()
    spec = parser.report_request(str(tmp_path))

    subprocess.run(
        [sys.executable, "-m", "pytest", str(suite), *spec.pytest_args, "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert spec.report_path is not None
    junit = Path(spec.report_path)
    assert junit.exists(), "pytest did not produce a JUnit report"
    return str(junit)


def test_parses_mixed_outcomes(tmp_path):
    junit = _make_junit(
        tmp_path,
        """
        import pytest

        def test_pass(): assert True
        def test_fail(): assert 1 == 2
        @pytest.mark.skip(reason="nope")
        def test_skip(): pass
    """,
    )
    result = JUnitResultParser().parse(junit, exit_code=1)
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
    junit = _make_junit(
        tmp_path,
        """
        def test_a(): assert True
        def test_b(): assert True
    """,
    )
    result = JUnitResultParser().parse(junit, exit_code=0)
    assert result.success is True
    assert result.passed == 2
    assert result.failed_node_ids == []


def test_parses_errors_in_fixture(tmp_path):
    junit = _make_junit(
        tmp_path,
        """
        import pytest

        @pytest.fixture
        def broken():
            raise RuntimeError("boom")

        def test_uses_broken(broken): pass
    """,
    )
    result = JUnitResultParser().parse(junit, exit_code=1)
    print(f"result: errors={result.errors}, success={result.success}")
    assert result.errors >= 1
    assert result.success is False


def test_missing_report_raises():
    with pytest.raises(ReportParseError):
        JUnitResultParser().parse("/nonexistent/junit.xml")


def test_malformed_report_raises(tmp_path):
    bad = tmp_path / "bad.xml"
    bad.write_text("<this is not <valid> xml")
    with pytest.raises(ReportParseError):
        JUnitResultParser().parse(str(bad))


def test_to_xcom_is_serializable(tmp_path):
    import json

    junit = _make_junit(tmp_path, "def test_x(): assert True")
    result = JUnitResultParser().parse(junit)
    payload = result.to_xcom()
    print(f"xcom payload: {payload}")
    json.dumps(payload)
    assert "cases" not in payload
    assert payload["success"] is True


def test_malformed_time_attribute_defaults_to_zero(tmp_path):
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuite name="pytest" tests="1" failures="0" errors="0" skipped="0">'
        '<testcase classname="m" name="test_a" time="not-a-number"></testcase>'
        "</testsuite>"
    )
    result = JUnitResultParser().parse(str(junit), exit_code=0)
    print(
        f"result: total={result.total}, passed={result.passed}, duration={result.duration}"
    )
    assert result.total == 1
    assert result.passed == 1
    assert result.duration == 0.0


def test_report_request_returns_expected_spec(tmp_path):
    spec = JUnitResultParser().report_request(str(tmp_path))
    print(f"spec: report_path={spec.report_path!r}, pytest_args={spec.pytest_args}")

    expected_path = str(tmp_path / "junit.xml")
    assert spec.report_path == expected_path
    assert spec.pytest_args == (
        f"--junitxml={expected_path}",
        "-o",
        "junit_logging=all",
    )


def test_report_request_uses_class_filename_constant(tmp_path):
    parser = JUnitResultParser()
    spec = parser.report_request(str(tmp_path))
    assert spec.report_path is not None
    assert spec.report_path.endswith(parser.REPORT_FILENAME)


def test_report_request_composes_path_inside_given_dir(tmp_path):
    nested = tmp_path / "deep" / "nested"
    nested.mkdir(parents=True)
    spec = JUnitResultParser().report_request(str(nested))
    assert spec.report_path is not None
    assert Path(spec.report_path).parent == nested


def test_parses_testsuites_root_element(tmp_path):
    """The parser must handle a <testsuites> wrapper (plural) as the root,
    not just the bare <testsuite> that pytest normally emits."""
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<?xml version="1.0" encoding="utf-8"?>'
        "<testsuites>"
        '<testsuite name="suite_a" tests="2">'
        '<testcase classname="mod_a" name="test_pass" time="0.1"/>'
        '<testcase classname="mod_a" name="test_fail" time="0.2">'
        '<failure message="assert False">long repr</failure>'
        "</testcase>"
        "</testsuite>"
        '<testsuite name="suite_b" tests="1">'
        '<testcase classname="mod_b" name="test_skip" time="0.0">'
        "<skipped/>"
        "</testcase>"
        "</testsuite>"
        "</testsuites>"
    )
    result = JUnitResultParser().parse(str(junit), exit_code=1)
    print(
        f"total={result.total} passed={result.passed} "
        f"failed={result.failed} skipped={result.skipped}"
    )
    assert result.total == 3
    assert result.passed == 1
    assert result.failed == 1
    assert result.skipped == 1
    assert result.errors == 0
    assert result.success is False


def test_parses_empty_report(tmp_path):
    """A valid but empty JUnit report (zero tests) must not crash."""
    junit = tmp_path / "junit.xml"
    junit.write_text('<testsuite name="pytest" tests="0"/>')
    result = JUnitResultParser().parse(str(junit), exit_code=0)
    assert result.total == 0
    assert result.success is True


# ---------------------------------------------------------------------------
# Narrowed parse exception handling (0.4.1 follow-up).
#
# Previously `except Exception as exc` caught everything from ParseError to
# MemoryError. We narrowed to (ET.ParseError, ValueError, OSError) so that:
#   * malformed XML  -> ReportParseError (as before)
#   * defusedxml security refusal -> ReportParseError (ValueError subclass)
#   * IO race -> ReportParseError
#   * anything else (MemoryError, AttributeError from our own bug, ...)
#     escapes uncaught so workers log the real problem.
# ---------------------------------------------------------------------------


def test_parse_error_on_malformed_xml_remains_report_parse_error(tmp_path):
    # Sanity: the most common case (truly malformed XML) still surfaces
    # as ReportParseError after narrowing -- this is what worked before
    # and must keep working.
    bad = tmp_path / "junit.xml"
    bad.write_text("<this is not valid xml")

    with pytest.raises(ReportParseError, match="Failed to parse JUnit report"):
        JUnitResultParser().parse(str(bad))
    print("[narrowed_except:malformed] ET.ParseError -> ReportParseError (common path)")


def test_defusedxml_security_exception_becomes_report_parse_error(
    tmp_path, monkeypatch
):
    from defusedxml.common import DefusedXmlException

    from airflow_pytest_operator.reporters import junit_parser

    junit_xml = tmp_path / "junit.xml"
    junit_xml.write_text("<testsuite name='x'/>")  # content doesn't matter

    def fake_parse(_path: str):
        raise DefusedXmlException("simulated XXE / DTD / entity refusal")

    monkeypatch.setattr(junit_parser, "_xml_parse", fake_parse)

    with pytest.raises(ReportParseError, match="Failed to parse JUnit report"):
        JUnitResultParser().parse(str(junit_xml))
    print(
        "[narrowed_except:defusedxml] DefusedXmlException (ValueError) "
        "-> ReportParseError"
    )


def test_io_race_becomes_report_parse_error(tmp_path, monkeypatch):
    from airflow_pytest_operator.reporters import junit_parser

    junit_xml = tmp_path / "junit.xml"
    junit_xml.write_text("<testsuite name='x'/>")

    def fake_parse(_path: str):
        raise PermissionError("simulated read-after-exists race")

    monkeypatch.setattr(junit_parser, "_xml_parse", fake_parse)

    with pytest.raises(ReportParseError) as exc_info:
        JUnitResultParser().parse(str(junit_xml))
    # The original OSError must be chained via __cause__ -- losing it
    # would defeat the point of catching it at all.
    assert isinstance(exc_info.value.__cause__, PermissionError)
    print(
        f"[narrowed_except:io_race] OSError -> ReportParseError "
        f"with __cause__={type(exc_info.value.__cause__).__name__}"
    )


def test_memory_error_is_not_swallowed_by_parse(tmp_path, monkeypatch):
    from airflow_pytest_operator.reporters import junit_parser

    junit_xml = tmp_path / "junit.xml"
    junit_xml.write_text("<testsuite name='x'/>")

    def fake_parse(_path: str):
        raise MemoryError("simulated allocation failure")

    monkeypatch.setattr(junit_parser, "_xml_parse", fake_parse)

    with pytest.raises(MemoryError, match="simulated"):
        JUnitResultParser().parse(str(junit_xml))
    print(
        "[narrowed_except:memory_error] MemoryError escapes the parser "
        "uncaught (not laundered into ReportParseError)"
    )


def test_attribute_error_is_not_swallowed_by_parse(tmp_path, monkeypatch):
    from airflow_pytest_operator.reporters import junit_parser

    junit_xml = tmp_path / "junit.xml"
    junit_xml.write_text("<testsuite name='x'/>")

    def fake_parse(_path: str):
        raise AttributeError("simulated bug: 'NoneType' has no attribute 'foo'")

    monkeypatch.setattr(junit_parser, "_xml_parse", fake_parse)

    with pytest.raises(AttributeError, match="simulated bug"):
        JUnitResultParser().parse(str(junit_xml))
    print(
        "[narrowed_except:attribute_error] AttributeError escapes the "
        "parser uncaught (bug-class exception preserves the real traceback)"
    )
