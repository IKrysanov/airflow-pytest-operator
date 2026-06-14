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

"""Cross-retry persistence for the ``failed_only`` retry strategy.

A single Airflow task cannot read its OWN XCom from a previous attempt --
Airflow clears a task instance's XCom at the start of every (re)run -- and
writing a *different* task's XCom from inside a task is not portable to
Airflow 3 (workers have no direct metadata-DB access). An Airflow Variable,
in contrast, is readable AND writable from within a task on both 2.x and 3.x,
survives the task's own retries, and can be deleted once no further retry will
read it. That is exactly the lifecycle ``failed_only`` needs, so the failed
node-id set is carried between native Airflow retries via a Variable.
"""

from .base import LastFailedStore
from .variable_store import (
    VariableLastFailedStore,
    is_final_attempt,
    last_failed_var_key,
)

__all__ = [
    "LastFailedStore",
    "VariableLastFailedStore",
    "last_failed_var_key",
    "is_final_attempt",
]
