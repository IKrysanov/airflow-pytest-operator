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

"""Tests for ``airflow_pytest_operator.utils.node_id``.

The converter takes the JUnit-style dotted form that parsers emit in
``failed_node_ids`` and turns it back into pytest CLI selectors. The
forward direction (slash → dotted) lives in
``JSONResultParser._split_nodeid``; this module is its inverse.

Most tests double as documentation of the conversion table -- each
example covers a representative case so a future reader can
``grep test_convert_<case>`` to see the expected behaviour.
"""

from __future__ import annotations

from airflow_pytest_operator import failed_selectors, node_id_to_pytest_args

# ---------------------------------------------------------------------------
# Conversion table: dotted -> slash. Each test covers one shape.
# ---------------------------------------------------------------------------


def test_convert_module_level_test():
    args = node_id_to_pytest_args(["tests.test_x::test_y"])
    print(f"[module_level] {args!r}")
    assert args == ["tests/test_x.py::test_y"]


def test_convert_class_based_test():
    args = node_id_to_pytest_args(["tests.test_x.TestClass::test_method"])
    print(f"[class_based] {args!r}")
    assert args == ["tests/test_x.py::TestClass::test_method"]


def test_convert_nested_class():
    args = node_id_to_pytest_args(["tests.test_x.TestOuter.TestInner::test_method"])
    print(f"[nested_class] {args!r}")
    assert args == ["tests/test_x.py::TestOuter::TestInner::test_method"]


def test_convert_nested_class_extra():
    args = node_id_to_pytest_args(["tests.test_x::TestInner::test_method"])
    print(f"[extra_class] {args!r}")
    assert args == ["tests/test_x.py::TestInner::test_method"]


def test_convert_parametrized_test():
    args = node_id_to_pytest_args(["tests.test_x::test_param[a-1]"])
    print(f"[parametrized] {args!r}")
    assert args == ["tests/test_x.py::test_param[a-1]"]


def test_convert_class_based_parametrized_test():
    args = node_id_to_pytest_args(["tests.test_x.TestClass::test_method[xyz-42]"])
    print(f"[class_parametrized] {args!r}")
    assert args == ["tests/test_x.py::TestClass::test_method[xyz-42]"]


def test_convert_deeply_nested_subdirs():
    args = node_id_to_pytest_args(["a.b.c.d.test_thing::test_y"])
    print(f"[deep_subdir] {args!r}")
    assert args == ["a/b/c/d/test_thing.py::test_y"]


def test_convert_lowercase_nested_class_stays_in_class_chain():
    args = node_id_to_pytest_args(
        ["tests.test_x.TestOuter.lowercase_inner::test_method"]
    )
    print(f"[lowercase_nested_inside_class] {args!r}")
    assert args == ["tests/test_x.py::TestOuter::lowercase_inner::test_method"]


# ---------------------------------------------------------------------------
# Idempotency: pre-converted slash-form must pass through unchanged.
# ---------------------------------------------------------------------------


def test_convert_already_slash_form_is_idempotent():
    inputs = [
        "tests/test_x.py::test_y",
        "tests/test_x.py::TestClass::test_method",
        "a/b/c/test_x.py::test_y[param]",
    ]
    args = node_id_to_pytest_args(inputs)
    print(f"[idempotent_slash] {args!r}")
    assert args == inputs


def test_convert_root_file_slash_form_passes_through():
    args = node_id_to_pytest_args(["test_x.py::test_y"])
    print(f"[idempotent_root_file] {args!r}")
    assert args == ["test_x.py::test_y"]


# ---------------------------------------------------------------------------
# Malformed inputs: return as-is rather than fabricating output.
# ---------------------------------------------------------------------------


def test_convert_no_separator_returns_unchanged():
    args = node_id_to_pytest_args(["just-a-string", "another"])
    print(f"[no_separator] {args!r}")
    assert args == ["just-a-string", "another"]


def test_convert_empty_classname_returns_unchanged():
    args = node_id_to_pytest_args(["::test_x"])
    print(f"[empty_classname] {args!r}")
    assert args == ["::test_x"]


def test_convert_only_class_segments_returns_unchanged():
    args = node_id_to_pytest_args(["TestOuter.TestInner::test_method"])
    print(f"[all_class_segments] {args!r}")
    assert args == ["TestOuter.TestInner::test_method"]


def test_convert_empty_input_returns_empty_list():
    print("[empty_input] ()")
    assert node_id_to_pytest_args([]) == []
    assert node_id_to_pytest_args(iter([])) == []


def test_convert_classname_that_is_only_dots_returns_unchanged():
    args = node_id_to_pytest_args(["...::test_x"])
    print(f"[only_dots_classname] {args!r}")
    assert args == ["...::test_x"]


# ---------------------------------------------------------------------------
# Configurable class_prefix.
# ---------------------------------------------------------------------------


def test_convert_with_custom_class_prefix_string():
    args = node_id_to_pytest_args(
        ["tests.test_x.SpecRequest::test_get_returns_200"],
        class_prefix="Spec",
    )
    print(f"[custom_prefix_str] {args!r}")
    assert args == ["tests/test_x.py::SpecRequest::test_get_returns_200"]


def test_convert_with_multiple_class_prefixes():
    items = [
        "tests.test_x.TestApi::test_get",
        "tests.test_x.SpecApi::test_post",
    ]
    args = node_id_to_pytest_args(items, class_prefix=("Test", "Spec"))
    print(f"[multiple_prefixes] {args!r}")
    assert args == [
        "tests/test_x.py::TestApi::test_get",
        "tests/test_x.py::SpecApi::test_post",
    ]


def test_convert_with_empty_class_prefix_disables_detection():
    args = node_id_to_pytest_args(
        ["MyProject.tests.test_x::test_y"],
        class_prefix="",
    )
    print(f"[empty_prefix_disables] {args!r}")
    assert args == ["MyProject/tests/test_x.py::test_y"]


def test_convert_with_empty_prefix_sequence_disables_detection():
    args = node_id_to_pytest_args(
        ["MyProject.tests.test_x::test_y"],
        class_prefix=[],
    )
    print(f"[empty_prefix_list_disables] {args!r}")
    assert args == ["MyProject/tests/test_x.py::test_y"]


# ---------------------------------------------------------------------------
# Documented limitations -- tests as living documentation.
# ---------------------------------------------------------------------------


def test_convert_capital_dir_with_default_prefix_is_a_known_caveat():
    args = node_id_to_pytest_args(["TestData.test_x::test_y"])
    print(f"[capital_dir_caveat] {args!r}")

    assert args == ["TestData.test_x::test_y"]

    args = node_id_to_pytest_args(["TestData.test_x::test_y"], class_prefix="")
    print(f"[capital_dir_workaround] {args!r}")
    assert args == ["TestData/test_x.py::test_y"]


def test_convert_non_test_class_name_with_default_prefix_misclassifies():
    args = node_id_to_pytest_args(["tests.test_x.MyApiTests::test_get"])
    print(f"[non_test_class_caveat] {args!r}")
    # ``MyApiTests`` doesn't match "Test*" -> treated as a path segment.
    assert args == ["tests/test_x/MyApiTests.py::test_get"]

    # With the right prefix the user gets the correct answer:
    args = node_id_to_pytest_args(
        ["tests.test_x.MyApiTests::test_get"], class_prefix="My"
    )
    print(f"[non_test_class_workaround] {args!r}")
    assert args == ["tests/test_x.py::MyApiTests::test_get"]


# ---------------------------------------------------------------------------
# Round-trip with the parser side.
# ---------------------------------------------------------------------------


def test_convert_round_trips_with_json_parser_split_nodeid():
    from airflow_pytest_operator.reporters.json_parser import _split_nodeid

    cases = [
        "tests/test_x.py::test_y",
        "tests/test_x.py::TestClass::test_method",
        "a/b/c/test_x.py::test_y[param]",
    ]
    for original_slash in cases:
        classname, name = _split_nodeid(original_slash)
        dotted = f"{classname}::{name}"
        [round_tripped] = node_id_to_pytest_args([dotted])
        print(
            f"[round_trip] {original_slash!r} -> dotted={dotted!r} -> {round_tripped!r}"
        )
        assert round_tripped == original_slash


# ---------------------------------------------------------------------------
# Generator / iterable input.
# ---------------------------------------------------------------------------


def test_convert_accepts_iterable_not_just_list():
    def gen():
        yield "tests.test_x::test_y"
        yield "tests.test_x.TestClass::test_method"

    args = node_id_to_pytest_args(gen())
    print(f"[generator_input] {args!r}")
    assert args == [
        "tests/test_x.py::test_y",
        "tests/test_x.py::TestClass::test_method",
    ]


def test_convert_returns_list_not_iterator():
    result = node_id_to_pytest_args(["tests.test_x::test_y"])
    print(f"[returns_list] type={type(result).__name__}")
    assert isinstance(result, list)
    assert result == ["tests/test_x.py::test_y"]


# ---------------------------------------------------------------------------
# End-to-end empirical proof.
# ---------------------------------------------------------------------------


def test_round_trip_actually_re_runs_only_failed_tests_via_real_pytest(
    tmp_path,
):
    import subprocess
    import sys
    import textwrap

    from airflow_pytest_operator import JUnitResultParser

    suite = tmp_path / "test_params.py"
    suite.write_text(
        textwrap.dedent(
            """
            import pytest

            @pytest.mark.parametrize("x", [1, 2, 3])
            def test_param(x):
                # x=2 fails
                assert x != 2

            @pytest.mark.parametrize("y", ["alpha", "beta"])
            @pytest.mark.parametrize("x", [1, 2])
            def test_combo(x, y):
                # combo(2, "alpha") fails
                assert (x, y) != (2, "alpha")

            class TestStuff:
                @pytest.mark.parametrize("ver", ["1.2.3", "2.0.0"])
                def test_with_version(self, ver):
                    # ver="2.0.0" fails -- DOTS inside the brackets
                    assert ver != "2.0.0"
            """
        ).strip()
    )

    # Pass 1: full suite. Capture failed_node_ids in the dotted shape.
    junit_1 = tmp_path / "j1.xml"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(suite),
            f"--junitxml={junit_1}",
            "-q",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    result_1 = JUnitResultParser().parse(str(junit_1), exit_code=1)
    dotted_ids = sorted(result_1.failed_node_ids)
    print(f"[round_trip:pass1] {result_1.failed} failures captured:")
    for nid in dotted_ids:
        print(f"  {nid!r}")

    # Sanity: the three we engineered to fail must be present.
    expected_failures = {
        "test_params::test_param[2]",
        "test_params::test_combo[2-alpha]",
        "test_params.TestStuff::test_with_version[2.0.0]",
    }
    assert set(dotted_ids) == expected_failures, (
        f"Suite produced unexpected failures; got {set(dotted_ids)!r}"
    )

    # Convert dotted -> slash via the public API.
    selectors = node_id_to_pytest_args(dotted_ids)
    print("\n[round_trip:convert] slash-form selectors:")
    for s in selectors:
        print(f"  {s!r}")

    junit_2 = tmp_path / "j2.xml"
    pass2 = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            *selectors,
            f"--junitxml={junit_2}",
            "-v",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    print(f"\n[round_trip:pass2] exit_code={pass2.returncode}")
    print(f"[round_trip:pass2] stdout tail:\n{pass2.stdout[-500:]}")

    result_2 = JUnitResultParser().parse(str(junit_2), exit_code=pass2.returncode)
    selected_ids = sorted(c.node_id for c in result_2.cases)
    print("\n[round_trip:pass2] selectors actually picked up:")
    for nid in selected_ids:
        print(f"  {nid!r}")

    assert selected_ids == dotted_ids, (
        f"Re-run selected a different set than the original failures.\n"
        f"Expected (failed in pass 1): {dotted_ids}\n"
        f"Actually re-run in pass 2:   {selected_ids}\n"
        f"Diff +: {sorted(set(selected_ids) - set(dotted_ids))}\n"
        f"Diff -: {sorted(set(dotted_ids) - set(selected_ids))}"
    )
    assert result_2.failed == 3
    assert result_2.passed == 0


# ---------------------------------------------------------------------------
# failed_selectors: XCom summary dict -> pytest selectors (run-all -> run-failed)
# ---------------------------------------------------------------------------


def test_failed_selectors_converts_failed_ids():
    summary = {
        "failed_node_ids": ["tests.test_x::test_y", "tests.test_z::test_w"],
    }
    sels = failed_selectors(summary)
    print(f"[failed_selectors:basic] {sels!r}")
    assert sels == ["tests/test_x.py::test_y", "tests/test_z.py::test_w"]


def test_failed_selectors_empty_when_no_failures():
    # An all-green run: the key is present but the list is empty.
    assert failed_selectors({"failed_node_ids": []}) == []


def test_failed_selectors_missing_key_returns_empty():
    # A summary without the key (e.g. a different/older shape) must not raise.
    assert failed_selectors({"passed": 3, "failed": 0}) == []


def test_failed_selectors_none_summary_returns_empty():
    # xcom_pull can return None (e.g. nothing pushed yet) -- handle it.
    assert failed_selectors(None) == []


def test_failed_selectors_none_failed_ids_returns_empty():
    # Defensive: key present but explicitly None.
    assert failed_selectors({"failed_node_ids": None}) == []


def test_failed_selectors_forwards_class_prefix():
    summary = {"failed_node_ids": ["tests.test_x.Suite::test_y"]}
    # class_prefix="Suite" makes the trailing segment a class, not a path part.
    sels = failed_selectors(summary, class_prefix="Suite")
    print(f"[failed_selectors:prefix] {sels!r}")
    assert sels == ["tests/test_x.py::Suite::test_y"]


def test_failed_selectors_real_summary_round_trips_from_to_xcom():
    # Use a real TestRunResult.to_xcom() so the test pins the actual contract
    # between the operator's XCom and this helper.
    from airflow_pytest_operator.models import CaseResult, TestRunResult

    result = TestRunResult(
        total=2,
        passed=1,
        failed=1,
        skipped=0,
        errors=0,
        duration=0.1,
        exit_code=1,
        cases=(
            CaseResult(name="test_ok", classname="tests.test_api", time=0.1,
                       outcome="passed"),
            CaseResult(name="test_bad", classname="tests.test_api", time=0.1,
                       outcome="failed"),
        ),
    )
    summary = result.to_xcom()
    print(f"[failed_selectors:from_to_xcom] summary={summary['failed_node_ids']!r}")
    assert failed_selectors(summary) == ["tests/test_api.py::test_bad"]
