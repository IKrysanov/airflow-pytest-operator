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
        f"result: total={result.total}, "
        f"passed={result.passed}, "
        f"failed={result.failed}, "
        f"skipped={result.skipped}, "
        f"errors={result.errors}, "
        f"success={result.success}"
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


def test_nodeid_normalized_to_junit_dotted_form(tmp_path):
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

    assert nid == "test_sample::test_x"
    assert ".py::" not in nid
    assert "/" not in nid


def test_xfail_is_treated_as_skipped(tmp_path):
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
    bad = tmp_path / "bad.json"
    bad.write_text("[1, 2, 3]")
    with pytest.raises(ReportParseError):
        JSONResultParser().parse(str(bad))


def test_to_xcom_is_serializable(tmp_path):
    report = _make_json(tmp_path, "def test_x(): assert True")
    result = JSONResultParser().parse(report)
    payload = result.to_xcom()
    print(f"xcom payload: {payload}")
    json.dumps(payload)
    assert "cases" not in payload
    assert payload["success"] is True


def test_report_request_returns_expected_spec(tmp_path):
    spec = JSONResultParser().report_request(str(tmp_path))
    print(f"spec: report_path={spec.report_path!r}, pytest_args={spec.pytest_args}")

    expected_path = str(tmp_path / "report.json")
    assert spec.report_path == expected_path
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


def test_report_request_path_is_absolute_for_relative_report_dir(tmp_path, monkeypatch):
    # Absolute path required: the runner may run pytest from a derived cwd, so a
    # relative report path would be written somewhere the runner cannot find.
    monkeypatch.chdir(tmp_path)
    spec = JSONResultParser(report_dir="reports").report_request("/fallback")
    assert Path(spec.report_path).is_absolute()
    assert spec.report_path == str(tmp_path / "reports" / "report.json")
    assert f"--json-report-file={spec.report_path}" in spec.pytest_args


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

    assert junit.total == json_result.total == 4
    assert junit.passed == json_result.passed == 2
    assert junit.failed == json_result.failed == 1
    assert junit.skipped == json_result.skipped == 1
    assert junit.errors == json_result.errors == 0


def _write_json(tmp_path: Path, payload: dict) -> str:
    p = tmp_path / "report.json"
    p.write_text(json.dumps(payload))
    return str(p)


def test_summary_falls_back_to_counted_cases_when_absent(tmp_path):
    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
            {"nodeid": "f.py::b", "outcome": "failed", "call": {"duration": 0.0}},
            {"nodeid": "f.py::c", "outcome": "skipped"},
            {"nodeid": "f.py::d", "outcome": "error"},
        ],
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=1)
    assert result.total == 4
    assert result.passed == 1
    assert result.failed == 1
    assert result.skipped == 1
    assert result.errors == 1


def test_malformed_duration_degrades_to_zero(tmp_path):
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
    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": None},
        ],
        "summary": {"total": 1, "passed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    assert result.cases[0].time == 0.0


def test_case_time_sums_setup_call_teardown(tmp_path):
    # Per-case time should reflect total work done on that case across all
    # three phases, not just the `call` phase. Without summation, a test
    # that fails in setup reports time=0.0 even when the setup took real
    # wall-clock time.
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::a",
                "outcome": "passed",
                "setup": {"duration": 0.10},
                "call": {"duration": 0.50},
                "teardown": {"duration": 0.05},
            },
        ],
        "summary": {"total": 1, "passed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    print(f"[case_time:sum] time={result.cases[0].time!r}")
    assert result.cases[0].time == pytest.approx(0.65)


def test_case_time_non_zero_on_setup_error(tmp_path):
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::a",
                "outcome": "error",
                "setup": {"duration": 0.20, "outcome": "failed"},
                # no "call" phase -- setup blew up before it
                "teardown": {"duration": 0.01},
            },
        ],
        "summary": {"total": 1, "errors": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=1)
    print(f"[case_time:setup_error] time={result.cases[0].time!r}")
    assert result.cases[0].time == pytest.approx(0.21)
    assert result.cases[0].outcome == "error"


def test_case_time_tolerates_malformed_phase_partial(tmp_path):
    # If one phase has a garbage duration, we should silently skip just
    # that phase and still sum the others -- not zero the whole case.
    payload = {
        "duration": 0.0,
        "tests": [
            {
                "nodeid": "f.py::a",
                "outcome": "passed",
                "setup": {"duration": "boom"},
                "call": {"duration": 0.40},
                "teardown": {"duration": 0.02},
            },
        ],
        "summary": {"total": 1, "passed": 1},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    print(f"[case_time:partial_malformed] time={result.cases[0].time!r}")
    assert result.cases[0].time == pytest.approx(0.42)


def test_unknown_outcome_maps_to_skipped_with_warning(tmp_path, caplog):
    import logging as _logging

    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "mystery", "call": {"duration": 0.0}},
            {"nodeid": "f.py::b", "outcome": "mystery", "call": {"duration": 0.0}},
            {"nodeid": "f.py::c", "outcome": "weirder", "call": {"duration": 0.0}},
        ],
    }
    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)

    assert all(c.outcome == "skipped" for c in result.cases)
    assert result.failed == 0
    assert result.errors == 0
    assert result.skipped == 3
    warnings = [r for r in caplog.records if r.levelno == _logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "mystery" in msg and "weirder" in msg


def test_message_pulled_from_setup_when_call_missing(tmp_path):
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
    assert result.cases[0].classname == "f"
    assert result.cases[0].name == "a"
    assert result.cases[0].node_id == "f::a"


def test_xpassed_counts_as_passed(tmp_path):
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


def test_non_list_tests_raises_report_parse_error(tmp_path):
    for bad_value in ("not a list", 42, {"nope": True}, None):
        path = _write_json(tmp_path, {"tests": bad_value, "duration": 0.0})
        with pytest.raises(ReportParseError, match="non-list 'tests'"):
            JSONResultParser().parse(path)


def test_malformed_summary_value_warns_once(tmp_path, caplog):
    import logging as _logging

    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
        ],
        "summary": {
            "total": 1,
            "passed": "yes please",
            "failed": "no thanks",
            "skipped": 0,
        },
    }
    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        result = JSONResultParser().parse(_write_json(tmp_path, payload))

    assert result.passed == 0
    assert result.failed == 0
    warnings = [r for r in caplog.records if r.levelno == _logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "passed" in msg
    assert "failed" in msg
    assert "non-numeric" in msg


def test_missing_summary_key_does_not_warn(tmp_path, caplog):
    import logging as _logging

    payload = {
        "duration": 0.0,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.0}},
        ],
        "summary": {"total": 1, "passed": 1},
    }
    with caplog.at_level(_logging.WARNING, logger="airflow_pytest_operator"):
        JSONResultParser().parse(_write_json(tmp_path, payload))
    warnings = [r for r in caplog.records if r.levelno == _logging.WARNING]
    assert warnings == []


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
    assert "(" not in msg
    assert "Skipped:" not in msg


def test_skipped_message_falls_back_on_schema_drift(tmp_path):
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
    assert "__import__" in result.cases[0].message


def test_skipped_via_pytest_skip_in_body(tmp_path):
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


def test_xpassed_strict_counts_as_failed_not_xpassed(tmp_path):
    report = _make_json(
        tmp_path,
        """
        import pytest

        @pytest.mark.xfail(strict=True)
        def test_actually_passes(): assert True
        """,
    )
    result = JSONResultParser().parse(report, exit_code=1)

    assert result.failed == 1
    assert result.passed == 0
    assert result.skipped == 0
    assert result.success is False


def test_failed_node_ids_match_junit_format(tmp_path):
    from airflow_pytest_operator.reporters import JUnitResultParser

    suite_src = """
        import pytest

        def test_a_fails(): assert False
        @pytest.mark.parametrize("x", [1, 2])
        def test_b_param(x): assert x == 1
        @pytest.mark.skip(reason="skipped on purpose")
        def test_c_skipped(): pass
    """
    suite = tmp_path / "test_dual.py"
    suite.write_text(textwrap.dedent(suite_src))

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

    junit_result = JUnitResultParser().parse(junit_spec.report_path, exit_code=1)
    json_result = JSONResultParser().parse(json_spec.report_path, exit_code=1)

    print(f"[parity:junit] {junit_result.failed_node_ids}")
    print(f"[parity:json]  {json_result.failed_node_ids}")

    assert sorted(junit_result.failed_node_ids) == sorted(json_result.failed_node_ids)
    assert all(".py" not in nid for nid in json_result.failed_node_ids)
    assert all("/" not in nid for nid in json_result.failed_node_ids)


def test_split_nodeid_module_level_test():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid("tests/test_x.py::test_y")
    print(f"[split:module_level] classname={cn!r} name={name!r}")
    assert cn == "tests.test_x"
    assert name == "test_y"


def test_split_nodeid_class_based_test():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid("tests/test_x.py::TestSomething::test_method")
    print(f"[split:class_based] classname={cn!r} name={name!r}")
    # Class nesting goes into classname, joined with '.'
    assert cn == "tests.test_x.TestSomething"
    assert name == "test_method"


def test_split_nodeid_parametrized_test():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid("tests/test_x.py::test_param[a-1]")
    print(f"[split:parametrized] classname={cn!r} name={name!r}")
    # Brackets stay in `name` -- pytest's JUnit XML does the same
    assert cn == "tests.test_x"
    assert name == "test_param[a-1]"


def test_split_nodeid_nested_subdir():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid("a/b/c/test_x.py::test_y")
    print(f"[split:nested_subdir] classname={cn!r} name={name!r}")
    assert cn == "a.b.c.test_x"
    assert name == "test_y"


def test_split_nodeid_malformed_keeps_text_in_name():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid("just-a-string")
    print(f"[split:malformed] classname={cn!r} name={name!r}")
    assert cn == ""
    assert name == "just-a-string"


def test_split_nodeid_handles_missing_py_suffix():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid("conftest::test_y")
    print(f"[split:no_py_suffix] classname={cn!r} name={name!r}")
    assert cn == "conftest"
    assert name == "test_y"


def test_split_nodeid_normalises_windows_backslashes():
    # On Windows, pytest can emit nodeids with backslash separators
    # (``tests\test_x.py::test_y``). Without normalisation, classname
    # would end up as ``tests\test_x`` and diverge from JUnit's dotted
    # form, breaking the parser parity contract.
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cn, name = _split_nodeid(r"tests\test_x.py::test_y")
    print(f"[split:windows] classname={cn!r} name={name!r}")
    assert cn == "tests.test_x"
    assert name == "test_y"

    cn, name = _split_nodeid(r"a\b\c\test_x.py::TestClass::test_method")
    print(f"[split:windows_nested] classname={cn!r} name={name!r}")
    assert cn == "a.b.c.test_x.TestClass"
    assert name == "test_method"


def test_summary_collected_fallback_does_not_override_normal_runs(tmp_path):
    payload = {
        "duration": 0.5,
        "tests": [
            {"nodeid": "f.py::a", "outcome": "passed", "call": {"duration": 0.1}},
            {"nodeid": "f.py::b", "outcome": "passed", "call": {"duration": 0.1}},
            {"nodeid": "f.py::c", "outcome": "failed", "call": {"duration": 0.1}},
        ],
        # If the fallback were applied blindly, total would become 99.
        "summary": {"total": 3, "passed": 2, "failed": 1, "collected": 99},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=1)
    print(
        f"[collected_fallback:negative] total={result.total} "
        f"(must be 3, summary.collected=99 is ignored on normal runs)"
    )
    assert result.total == 3


def test_summary_collected_zero_keeps_total_zero(tmp_path):
    payload = {
        "duration": 0.01,
        "tests": [],
        "summary": {"total": 0, "collected": 0},
    }
    result = JSONResultParser().parse(_write_json(tmp_path, payload), exit_code=0)
    print(f"[collected_fallback:zero] total={result.total}")
    assert result.total == 0


def test_dry_run_with_json_parser_reports_collected_count(tmp_path):
    suite = tmp_path / "test_dual.py"
    suite.write_text(
        textwrap.dedent(
            """
            def test_a(): assert True
            def test_b(): assert True
            def test_c(): assert True
            """
        ).strip()
    )

    spec = JSONResultParser().report_request(str(tmp_path))
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(suite),
            "--collect-only",
            *spec.pytest_args,
            "-q",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    result = JSONResultParser().parse(spec.report_path, exit_code=0)
    print(
        f"[dry_run:json_parser] total={result.total} "
        f"cases={len(result.cases)} success={result.success}"
    )

    assert result.total == 3
    assert len(result.cases) == 0
    assert result.success is True
    assert result.failed_node_ids == []
