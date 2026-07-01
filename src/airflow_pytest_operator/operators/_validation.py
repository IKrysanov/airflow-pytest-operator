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
    if test_retry_strategy not in RETRY_STRATEGIES:
        raise ValueError(
            "test_retry_strategy must be one of 'all', 'failed_only'; "
            f"got {test_retry_strategy!r}"
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


def validate_env(env: Any) -> None:
    # env keys/values become child env vars and must be strings: a non-str
    # (e.g. a bare True) otherwise fails deep in os.fsencode. Reject here,
    # naming the offending key.
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
