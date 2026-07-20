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


"""XML hardening for the JUnit parser.

defusedxml is optional (the [secure-xml] extra). Without it the stdlib parser is
used, which is vulnerable to entity-expansion (billion laughs) -- so the
downgrade must be visible in the log rather than silent."""

from __future__ import annotations

import logging

import pytest

from airflow_pytest_operator.exceptions import ReportParseError
from airflow_pytest_operator.reporters import JUnitResultParser
from airflow_pytest_operator.reporters import junit_parser as jp

_OK_REPORT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" time="0.1">
<testcase classname="tests.test_x" name="test_a" time="0.1"/>
</testsuite></testsuites>
"""

# Classic billion-laughs: each entity references the previous one ten times.
_BOMB_REPORT = """<?xml version="1.0"?>
<!DOCTYPE testsuites [
  <!ENTITY a "aaaaaaaaaa">
  <!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">
  <!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">
  <!ENTITY d "&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;">
]>
<testsuites><testsuite name="pytest" time="0.1">
<testcase classname="tests.test_x" name="&d;" time="0.1"/>
</testsuite></testsuites>
"""


def _report(tmp_path, text, name="junit.xml"):
    p = tmp_path / name
    p.write_text(text)
    return str(p)


def test_hardened_parser_is_used_when_defusedxml_is_installed():
    print(f"[xml:hardened] _HARDENED_XML={jp._HARDENED_XML}")
    assert jp._HARDENED_XML is True


def test_entity_expansion_is_refused(tmp_path):
    # The actual protection: a bomb must raise, not expand into memory.
    path = _report(tmp_path, _BOMB_REPORT)
    with pytest.raises(ReportParseError):
        JUnitResultParser().parse(path)


def test_no_warning_when_hardened(tmp_path, caplog):
    path = _report(tmp_path, _OK_REPORT)
    with caplog.at_level(logging.WARNING):
        JUnitResultParser().parse(path)
    print(f"[xml:no-warn] {caplog.text!r}")
    assert "secure-xml" not in caplog.text


def test_unhardened_fallback_warns(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(jp, "_HARDENED_XML", False)
    monkeypatch.setattr(jp, "_UNHARDENED_WARNED", False)
    path = _report(tmp_path, _OK_REPORT)
    with caplog.at_level(logging.WARNING):
        result = JUnitResultParser().parse(path)
    print(f"[xml:warn] {caplog.text!r}")
    assert "secure-xml" in caplog.text
    assert result.total == 1  # still parses; only the warning is new


def test_unhardened_warning_is_emitted_once(tmp_path, caplog, monkeypatch):
    # A per-parse warning would spam the task log on a sharded DAG.
    monkeypatch.setattr(jp, "_HARDENED_XML", False)
    monkeypatch.setattr(jp, "_UNHARDENED_WARNED", False)
    path = _report(tmp_path, _OK_REPORT)
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            JUnitResultParser().parse(path)
    hits = caplog.text.count("secure-xml")
    print(f"[xml:warn-once] hits={hits}")
    assert hits == 1


def test_import_guard_is_narrow():
    # A broken defusedxml must fail loudly, not silently downgrade security,
    # so the guard catches ImportError only.
    import inspect

    src = inspect.getsource(jp)
    print(f"[xml:guard] broad_except={'except Exception' in src}")
    assert "except Exception" not in src
    assert "except ImportError" in src
