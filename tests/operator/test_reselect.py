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

"""suite_root / reselect: locating rootdir-relative node-ids in the original
targets' tree, so reruns find the failed tests from any worker cwd and keep
the full run's rootdir."""

from __future__ import annotations

import pytest

from airflow_pytest_operator.operators._reselect import reselect, suite_root


@pytest.fixture
def tree(tmp_path, monkeypatch):
    """proj/tests/{test_a.py, sub/test_b.py} + an unrelated cwd, entered."""
    tests = tmp_path / "proj" / "tests"
    (tests / "sub").mkdir(parents=True)
    (tests / "test_a.py").write_text("def test_a(): pass\n")
    (tests / "sub" / "test_b.py").write_text("def test_b(): pass\n")
    elsewhere = tmp_path / "worker_cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    return tests


# -- suite_root -------------------------------------------------------------


def test_suite_root_of_a_directory_is_itself(tree):
    assert suite_root(str(tree)) == str(tree)


def test_suite_root_of_a_file_or_node_id_is_its_directory(tree):
    assert suite_root(str(tree / "sub" / "test_b.py")) == str(tree / "sub")
    assert suite_root(f"{tree / 'test_a.py'}::test_a") == str(tree)


def test_suite_root_of_several_targets_is_their_common_directory(tree):
    targets = [f"{tree / 'test_a.py'}::test_a", str(tree / "sub" / "test_b.py")]
    assert suite_root(targets) == str(tree)


def test_suite_root_resolves_relative_targets_against_the_worker_cwd(tree, monkeypatch):
    monkeypatch.chdir(tree.parent)
    assert suite_root("tests") == str(tree)


def test_suite_root_skips_targets_missing_on_this_worker(tree):
    # A custom runner may resolve these elsewhere; nothing to anchor on here.
    assert suite_root("no/such/dir") is None
    assert suite_root(["no/such/dir", str(tree)]) == str(tree)
    assert suite_root("::test_x") is None


# -- reselect ---------------------------------------------------------------


def test_rootdir_equal_to_the_targets(tree):
    # No config file above the tests: rootdir is the tests dir itself, so ids
    # carry no "tests." prefix -- the case that ran nothing before anchoring.
    plan = reselect(["test_a::test_a", "sub.test_b::test_b"], str(tree))
    assert plan.selectors == [
        f"{tree / 'test_a.py'}::test_a",
        f"{tree / 'sub' / 'test_b.py'}::test_b",
    ]
    assert (plan.missing, plan.rootdir, plan.root) == ([], str(tree), str(tree))


def test_rootdir_above_the_targets(tree):
    # A pytest.ini in proj/ makes proj the rootdir: ids start with "tests.".
    (tree.parent / "pytest.ini").write_text("[pytest]\n")
    plan = reselect(["tests.test_a::test_a"], str(tree))
    assert plan.selectors == [f"{tree / 'test_a.py'}::test_a"]
    assert plan.rootdir == str(tree.parent)


def test_a_base_above_the_root_is_pinned_only_with_a_config_file(tree):
    # Without a config file pytest would not have used proj/ as its rootdir, so
    # the ids resolve from there but nothing is pinned -- a hand-edited
    # failed_only Variable must not choose the rootdir (and with it, which
    # conftest.py files load). The selectors themselves still work.
    plan = reselect(["tests.test_a::test_a"], str(tree))
    assert plan.selectors == [f"{tree / 'test_a.py'}::test_a"]
    assert plan.rootdir is None

    (tree.parent / "tox.ini").write_text("[tox]\n")
    assert reselect(["tests.test_a::test_a"], str(tree)).rootdir == str(tree.parent)


def test_selector_suffix_is_kept_verbatim(tree):
    plan = reselect(["sub.test_b::TestC::test_x[a::b]"], str(tree))
    assert plan.selectors == [f"{tree / 'sub' / 'test_b.py'}::TestC::test_x[a::b]"]


def test_targets_off_this_worker_leave_selectors_unchecked():
    # A custom runner / explicit cwd resolves them elsewhere: nothing to check.
    plan = reselect(["tests.test_a::test_a"], "no/such/dir")
    assert plan.selectors == ["tests/test_a.py::test_a"]
    assert (plan.missing, plan.rootdir, plan.root) == ([], None, None)


def test_ids_without_a_file_are_missing(tree):
    # A collection error has no "::test" part; a renamed file has no file.
    plan = reselect(["test_a::test_a", "test_broken", "gone::test_x"], str(tree))
    assert plan.selectors == [f"{tree / 'test_a.py'}::test_a"]
    assert plan.missing == ["test_broken", "gone::test_x"]


def test_a_match_outside_the_root_is_not_taken(tree):
    # proj/other/test_z.py exists, but every collected file lives under the
    # targets' tree -- a stray match beside it is never the test that failed.
    other = tree.parent / "other"
    other.mkdir()
    (other / "test_z.py").write_text("def test_z(): pass\n")
    assert reselect(["other.test_z::test_z"], str(tree)).missing == [
        "other.test_z::test_z"
    ]


def test_paths_escaping_the_root_are_missing(tree):
    # The failed_only set comes from an Airflow Variable: neither an absolute
    # path nor a "../" one may point pytest at a file outside the suite.
    (tree.parent / "evil.py").write_text("")
    evil = f"{tree.parent / 'evil.py'}::x"
    plan = reselect(["../evil.py::x", evil], str(tree))
    assert plan.selectors == []
    assert plan.missing == ["../evil.py::x", evil]


# -- hardening: the ids may come from a hand-edited Airflow Variable ---------


def test_a_symlink_out_of_the_suite_is_not_followed(tree, tmp_path):
    # The file exists and the path *looks* like it is inside the suite, but it
    # resolves outside it -- pytest would import and execute it.
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "evil.py").write_text("raise SystemExit('pwned')\n")
    (tree / "link").symlink_to(outside)

    plan = reselect(["link.evil::x"], str(tree))

    assert plan.selectors == []
    assert plan.missing == ["link.evil::x"]


def test_the_stored_ids_cannot_choose_the_rootdir(tree):
    # An id deep enough to resolve only from an ancestor would pin --rootdir
    # there, handing pytest every conftest.py from that directory down.
    plan = reselect(["proj.tests.test_a::test_a"], str(tree))

    assert plan.selectors == [f"{tree / 'test_a.py'}::test_a"]  # still re-runnable
    assert plan.rootdir is None  # but not on a rootdir of the id's choosing


def test_a_selector_is_never_split_into_extra_arguments(tree):
    # Whatever is stored stays ONE pytest argument: no shell, no word splitting.
    nasty = "sub.test_b::test_b[a b; rm -rf /]"

    plan = reselect([nasty], str(tree))

    assert plan.selectors == [f"{tree / 'sub' / 'test_b.py'}::test_b[a b; rm -rf /]"]
