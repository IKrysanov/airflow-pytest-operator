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

from typing import Any

from ..stores import LastFailedStore
from ._constants import DIST_MODES, PARALLEL_KEYWORDS, RETRY_STRATEGIES


def validate_test_retry_strategy(test_retry_strategy: str) -> None:
    # Type first: a non-str reached the frozenset lookup below and an unhashable
    # one (a list) died there with a bare "unhashable type", naming nothing.
    if not isinstance(test_retry_strategy, str):
        raise TypeError(
            "test_retry_strategy must be a str -- 'all' or 'failed_only'; "
            f"got {type(test_retry_strategy).__name__}"
        )
    if test_retry_strategy not in RETRY_STRATEGIES:
        raise ValueError(
            "test_retry_strategy must be one of 'all', 'failed_only'; "
            f"got {test_retry_strategy!r}"
        )


def validate_test_path(test_path: Any) -> None:
    # Deliberately only a None check. test_path is a template field, and the
    # README's failed_only example passes an XComArg straight into it, so a type
    # check here would reject a documented pattern. None is unambiguous (the
    # parameter is required and has no default) and otherwise surfaces on the
    # worker as "'NoneType' object is not iterable" from inside the runner.
    # Empty / blank values are the runner's own fail-closed check, which already
    # reports them clearly and sees the post-template value.
    if test_path is None:
        raise TypeError(
            "test_path is required: a pytest target (file, directory or "
            "node-id), or a sequence of them; got None"
        )


def validate_pytest_args(pytest_args: Any) -> None:
    # A str is iterable, so ``list("--verbose")`` silently becomes nine
    # single-character arguments that reach pytest as garbage -- the one input
    # here that corrupts a run instead of failing it. A dict is iterable too and
    # silently degrades to its keys. Only these two are rejected: any other
    # container may legitimately be an XComArg on this template field.
    if isinstance(pytest_args, (str, bytes)):
        raise TypeError(
            "pytest_args must be a sequence of separate arguments, not a "
            f'single string: pass ["-k", "smoke"], not {pytest_args!r} '
            "(a string is iterated character by character)"
        )
    if isinstance(pytest_args, dict):
        raise TypeError(
            "pytest_args must be a sequence of arguments; a dict would be "
            "silently reduced to its keys and its values dropped"
        )


def validate_markers_keyword(markers: str | None, keyword: str | None) -> None:
    # Only the type is checked here; an empty/blank value is left to execute()
    # (a template may render to "" and be skipped).
    for _name, _value in (("markers", markers), ("keyword", keyword)):
        if _value is not None and not isinstance(_value, str):
            raise TypeError(
                f"{_name} must be a str (a pytest -m/-k expression) or None; "
                f"got {type(_value).__name__}"
            )


def validate_rerun_failed(rerun_failed: int) -> None:
    # bool is an int subclass -- reject it explicitly (it's not a count).
    if isinstance(rerun_failed, bool) or not isinstance(rerun_failed, int):
        raise TypeError(
            f"rerun_failed must be an int (not bool); got {type(rerun_failed).__name__}"
        )
    if rerun_failed < 0:
        raise ValueError(
            f"rerun_failed must be a non-negative integer; got {rerun_failed!r}"
        )


def validate_parallel_dist(parallel: int | str | None, dist: str | None) -> None:
    # parallel: xdist worker count. None = serial; int must be >= 1;
    # "auto"/"logical" are xdist keywords. bool rejected explicitly.
    if parallel is not None:
        if isinstance(parallel, bool):
            raise TypeError(
                "parallel must be an int or 'auto'/'logical' (not bool); "
                f"got {type(parallel).__name__}"
            )
        if isinstance(parallel, int):
            if parallel < 1:
                raise ValueError(
                    f"parallel must be >= 1 (or None to disable); got {parallel!r}"
                )
        elif isinstance(parallel, str):
            if parallel not in PARALLEL_KEYWORDS:
                raise ValueError(
                    "parallel string must be one of 'auto', 'logical'; "
                    f"got {parallel!r}"
                )
        else:
            raise TypeError(
                "parallel must be an int, 'auto'/'logical', or None; "
                f"got {type(parallel).__name__}"
            )
    # dist: xdist scheduler mode. Require parallel -- --dist is inert without
    # -n, so reject it alone rather than silently no-op.
    if dist is not None:
        if not isinstance(dist, str):
            raise TypeError(
                "dist must be a str (an xdist scheduler mode) or None; "
                f"got {type(dist).__name__}"
            )
        if dist not in DIST_MODES:
            raise ValueError(
                f"dist must be one of {', '.join(sorted(DIST_MODES))}; got {dist!r}"
            )
        if parallel is None:
            raise ValueError(
                "dist requires parallel to be set (a worker count or "
                "'auto'/'logical'); --dist has no effect without -n."
            )


def validate_coverage(coverage: bool) -> None:
    # coverage is a bool toggle for the pytest-cov splice. Reject a non-bool
    # (no truthy ints) so a stray ``coverage=1`` does not silently enable it.
    if not isinstance(coverage, bool):
        raise TypeError(
            "coverage must be a bool (True to enable pytest-cov coverage "
            f"measurement, False to disable); got {type(coverage).__name__}"
        )


def validate_cache(cache: bool) -> None:
    # cache is a bool toggle for pytest's cacheprovider plugin. Reject a
    # non-bool (no truthy ints) so a stray ``cache=0`` does not silently
    # disable the cache, matching the ``coverage`` convention above.
    if not isinstance(cache, bool):
        raise TypeError(
            "cache must be a bool (True to leave pytest's cacheprovider "
            "enabled, False to disable it with -p no:cacheprovider); "
            f"got {type(cache).__name__}"
        )


def validate_cov_fail_under(cov_fail_under: float | None) -> None:
    # cov_fail_under: optional coverage gate, a fraction in [0, 1] compared
    # against the same value pushed to XCom under ``coverage``. Reject bool (a
    # stray True is not a threshold) and a percentage-style value (>1) with a
    # pointed hint -- "fail under 80" is a very natural mistake.
    if cov_fail_under is None:
        return
    if isinstance(cov_fail_under, bool) or not isinstance(cov_fail_under, (int, float)):
        raise TypeError(
            "cov_fail_under must be a float in [0, 1] (a coverage "
            f"fraction) or None; got {type(cov_fail_under).__name__}"
        )
    if not 0.0 <= cov_fail_under <= 1.0:
        raise ValueError(
            "cov_fail_under is a fraction in [0, 1]; use 0.8 for 80%. "
            f"Got {cov_fail_under!r}"
        )


def validate_min_pass_rate(min_pass_rate: float | None) -> None:
    # Failure-tolerance gate: a fraction in [0, 1] compared against the run's
    # ``passed / (total - skipped)``. Same shape and footguns as cov_fail_under.
    if min_pass_rate is None:
        return
    if isinstance(min_pass_rate, bool) or not isinstance(min_pass_rate, (int, float)):
        raise TypeError(
            "min_pass_rate must be a float in [0, 1] (the fraction of executed "
            f"tests that must pass) or None; got {type(min_pass_rate).__name__}"
        )
    # Also rejects NaN, which must not pass: it compares False against every
    # rate, silently disabling the gate.
    if not 0.0 <= min_pass_rate <= 1.0:
        raise ValueError(
            "min_pass_rate is a fraction in [0, 1]; use 0.95 for 95%. "
            f"Got {min_pass_rate!r}"
        )


def validate_max_failed(max_failed: int | None) -> None:
    # Absolute cap on failed + errors. A count, so bool (an int subclass) is
    # rejected explicitly; 0 is the way to tolerate nothing.
    if max_failed is None:
        return
    if isinstance(max_failed, bool) or not isinstance(max_failed, int):
        raise TypeError(
            "max_failed must be an int (the maximum number of failed + errored "
            f"tests tolerated) or None, not bool; got {type(max_failed).__name__}"
        )
    if max_failed < 0:
        raise ValueError(
            "max_failed must be a non-negative integer; use 0 to tolerate no "
            f"failure at all. Got {max_failed!r}"
        )


def validate_threshold_combination(
    min_pass_rate: float | None, max_failed: int | None
) -> None:
    # No pair of the AND-ed gates is contradictory -- a fully green run satisfies
    # any of them -- so the only thing to reject is a gate that can never decide,
    # the same reasoning as "dist requires parallel" above. min_pass_rate=1.0
    # needs every executed test to pass, so any failure breaches it before the
    # cap is consulted: "tolerate 5" would silently mean "tolerate none".
    # max_failed=0 is allowed -- it says exactly what min_pass_rate=1.0 says.
    if min_pass_rate == 1.0 and max_failed is not None and max_failed > 0:
        raise ValueError(
            f"max_failed={max_failed} can never apply: min_pass_rate=1.0 "
            "requires every executed test to pass, so any failure breaches it "
            "first. Either lower min_pass_rate (e.g. 0.95 to really tolerate "
            f"some failures alongside max_failed={max_failed}), or drop "
            "max_failed and keep the all-must-pass gate."
        )


def validate_store(store: Any) -> None:
    # Fail fast on a bad store: the runtime_checkable protocol rejects anything
    # missing read/write/delete (structural -- methods only).
    if store is not None and not isinstance(store, LastFailedStore):
        raise TypeError(
            "store must implement the LastFailedStore protocol -- an object "
            "with read(key), write(key, ids) and delete(key) methods, e.g. "
            "the default VariableLastFailedStore(). "
            f"Got {type(store).__name__}."
        )


def validate_collaborators(runner: Any, parser: Any) -> None:
    # Structural, like validate_store: PytestRunner / ResultParser are ABCs, but
    # the operator only ever relies on the method contract and the test suite
    # proves it with a runner that subclasses neither -- an isinstance check
    # would outlaw that documented duck-typed extension point. Without this the
    # mistake surfaces on the worker mid-execute as a bare AttributeError.
    for name, obj, methods in (
        ("runner", runner, ("run", "cleanup")),
        ("parser", parser, ("report_request", "parse")),
    ):
        if obj is None:
            continue
        missing = [m for m in methods if not callable(getattr(obj, m, None))]
        if missing:
            raise TypeError(
                f"{name} must be an object with {' and '.join(methods)}() "
                f"methods; {type(obj).__name__} is missing "
                f"{', '.join(missing)}."
            )


def validate_env(env: Any) -> None:
    # env keys/values become child env vars. Anything the OS itself refuses is
    # rejected here, naming the offending key: otherwise it surfaces on the
    # worker mid-execute as a bare "embedded null byte" from deep inside
    # os.fsencode, with nothing tying it to this parameter. The content rules
    # are exactly the OS's own (a name is non-empty and free of '=' and NUL, a
    # value is free of NUL) -- no stricter, so shells' looser naming still works.
    if env is None:
        return
    if not isinstance(env, dict):
        raise TypeError(
            f"env must be a dict[str, str] or None; got {type(env).__name__}"
        )
    for key, value in env.items():
        if not isinstance(key, str):
            raise TypeError(
                f"env keys must be str (env vars are strings); "
                f"got {type(key).__name__} ({key!r})"
            )
        if not isinstance(value, str):
            raise TypeError(
                f"env[{key!r}] must be a str (env vars are strings); "
                f"got {type(value).__name__}"
            )
        if not key:
            raise ValueError(
                "env keys must not be empty: an unnamed variable cannot be "
                "exported and would be dropped silently"
            )
        if "=" in key:
            raise ValueError(
                f"env key {key!r} must not contain '=': the OS uses it to "
                "separate a variable's name from its value"
            )
        if "\0" in key:
            raise ValueError(
                f"env key {key!r} must not contain a NUL character (env "
                "strings are NUL-terminated)"
            )
        if "\0" in value:
            raise ValueError(
                f"env[{key!r}] must not contain a NUL character (env strings "
                "are NUL-terminated)"
            )
