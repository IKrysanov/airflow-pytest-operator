# airflow-pytest-operator

Run a `pytest` suite as an Airflow task. The operator executes your tests in a child process, parses the JUnit report into a structured result, pushes a summary to XCom, and fails the task when tests fail (configurable).

Works on **Airflow 2.x and 3.x** — all version-specific imports are isolated in a single compatibility module, so one wheel supports both.

**Package**

| Badge | What it tells you |
|:------|:------------------|
| [![PyPI version](https://img.shields.io/pypi/v/airflow-pytest-operator.svg)](https://pypi.org/project/airflow-pytest-operator/) | Latest release on PyPI — `pip install airflow-pytest-operator` |
| [![Downloads/month](https://static.pepy.tech/badge/airflow-pytest-operator/month)](https://pepy.tech/projects/airflow-pytest-operator) | Downloads from PyPI in the last month (via pepy) |
| [![Python versions](https://img.shields.io/pypi/pyversions/airflow-pytest-operator.svg)](https://pypi.org/project/airflow-pytest-operator/) | Supported Python versions (3.10+) |
| [![Airflow](https://img.shields.io/badge/Airflow-2.x%20%7C%203.x-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/) | Compatible Airflow majors — one wheel for 2.x **and** 3.x |
| [![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) | Distributed under the Apache-2.0 licence |

**Quality &amp; build**

| Badge | What it tells you |
|:------|:------------------|
| [![CI](https://github.com/IKrysanov/airflow-pytest-operator/actions/workflows/ci.yml/badge.svg)](https://github.com/IKrysanov/airflow-pytest-operator/actions/workflows/ci.yml) | Build & test suite (lint, types, unit, integration) on `main` |
| [![codecov](https://codecov.io/gh/IKrysanov/airflow-pytest-operator/branch/main/graph/badge.svg)](https://codecov.io/gh/IKrysanov/airflow-pytest-operator) | Test coverage of the package |
| [![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/) | Fully type-checked with mypy `--strict` |
| [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) | Linted & formatted with Ruff |
| [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/IKrysanov/airflow-pytest-operator/badge)](https://scorecard.dev/viewer/?uri=github.com/IKrysanov/airflow-pytest-operator) | OpenSSF supply-chain security score |

## Table of Contents

- [Why a child process](#why-a-child-process)
- [Install](#install)
  - [Quick install](#quick-install)
  - [Installing in Docker / constrained environments](#installing-in-docker--constrained-environments)
- [Verifying the release](#verifying-the-release)
  - [Path 1 — verify the PyPI artifact (PEP 740)](#path-1--verify-the-pypi-artifact-pep-740)
  - [Path 2 — verify the GitHub Release artifact (Sigstore bundle)](#path-2--verify-the-github-release-artifact-sigstore-bundle)
- [Usage](#usage)
- [Passing values from upstream tasks into your tests](#passing-values-from-upstream-tasks-into-your-tests)
- [Constructor options](#constructor-options)
- [.env files](#env-files)
- [Debugging a run (verbose diagnostics)](#debugging-a-run-verbose-diagnostics)
- [Streaming pytest output (stream_output)](#streaming-pytest-output-stream_output)
- [pytest config, plugins, and Allure](#pytest-config-plugins-and-allure)
- [Selecting tests (markers / keyword)](#selecting-tests-markers--keyword)
- [Parallel execution (parallel / dist)](#parallel-execution-parallel--dist)
- [Coverage (coverage)](#coverage-coverage)
- [Sharding across workers (dynamic task mapping)](#sharding-across-workers-dynamic-task-mapping)
- [Report location & cleanup](#report-location--cleanup)
- [Cancellation and timeouts](#cancellation-and-timeouts)
- [Dry-run mode](#dry-run-mode)
- [Retry strategy (failed-only reruns)](#retry-strategy-failed-only-reruns)
- [Where do the tests live?](#where-do-the-tests-live)
- [Built-in parsers](#built-in-parsers)
- [Extending it](#extending-it)
  - [Custom parser](#custom-parser)
  - [Custom runner](#custom-runner)
- [Architecture](#architecture)
- [Development](#development)
- [Changelog](#changelog)
- [License](#license)

## Why a child process

Tests run via `{sys.executable} -m pytest`, i.e. in the **same virtualenv / interpreter as the Airflow worker** (same dependencies), but in a **child process**. This keeps pytest's global-state mutations (`sys.modules`, plugins, cwd, logging) out of the long-lived worker while still satisfying "same environment" semantics. A crashing or segfaulting test can't take the worker down, and the child can be killed cleanly on timeout or task termination.

## Install

### Quick install

```bash
pip install airflow-pytest-operator
```

**Worker extras** — install alongside the operator on every Airflow worker that
will run test tasks. Pick the ones your tasks use (combine freely):

| Extra | Installs | When you need it |
|-------|----------|------------------|
| `pytest` | `pytest` | Almost always — the worker runs `python -m pytest`, so pytest must be importable there. |
| `xdist` | `pytest`, `pytest-xdist` | The `parallel=` / `dist=` parameters (parallel runs on the worker). See [Parallel execution](#parallel-execution-parallel--dist). |
| `pytest-allure` | `pytest`, `allure-pytest` | Generating Allure reports (`--alluredir`). Viewing them also needs the Allure CLI (Java); see [pytest config, plugins, and Allure](#pytest-config-plugins-and-allure). |
| `json-report` | `pytest-json-report` | Using the built-in `JSONResultParser`. See [Built-in parsers](#built-in-parsers). |
| `dotenv` | `python-dotenv` | The `env_file=` parameter. See [.env files](#env-files). |
| `secure-xml` | `defusedxml` | Hardened XML parsing of untrusted JUnit reports (recommended in production). |
| `airflow2` / `airflow3` | `apache-airflow` 2.x / 3.x | Pin a compatible Airflow when you want resolution help (Airflow is otherwise **not** a hard dependency — see below). |

```bash
# one extra
pip install "airflow-pytest-operator[xdist]"

# combine as needed
pip install "airflow-pytest-operator[pytest,xdist,secure-xml]"
```

Airflow itself is **not** a hard dependency — the package installs into your existing Airflow environment. Use the `airflow2` / `airflow3` extra above only when you want pip to pin a compatible Airflow for you.

### Installing in Docker / constrained environments

In an Airflow Docker image, install the package **with Airflow's official constraint file** so dependency resolution matches your Airflow version exactly. Make sure the build args are actually set — an empty `AIRFLOW_VERSION`/`PYTHON_VERSION` produces an invalid constraint URL and the build fails:

```dockerfile
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir "airflow-pytest-operator" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

The package itself pins nothing (`dependencies = []`), so any resolution conflict comes from your wider environment; the constraint file is the standard way to keep it reproducible.

## Verifying the release

Each PyPI release is published from GitHub Actions via PyPI's
[Trusted Publishing](https://docs.pypi.org/trusted-publishers/) and ships
with a [PEP 740](https://peps.python.org/pep-0740/) **Sigstore attestation**
that cryptographically binds every wheel and sdist to a specific commit and
workflow in this repository. PyPI verifies the attestation at upload time
and shows the source repository in the release's *Verified details*. You can
also verify it yourself before installing, which protects against tampering
between PyPI and your machine.

PyPI verifies the attestation at upload time and surfaces the link back to
this repository in the release's *Verified details*, so the common case
(`pip install airflow-pytest-operator`) already gives you that assurance
through PyPI. For deeper verification before installing — for example in a
security-sensitive or air-gapped environment — there are two independent
paths, both rooted in the same Sigstore public-good instance.

### Path 1 — verify the PyPI artifact (PEP 740)

Each PyPI release carries a PEP 740 attestation that ties the wheel and
sdist to the exact `release.yml` run that produced them. The
[`pypi-attestations`](https://pypi.org/project/pypi-attestations/) CLI
fetches the artifact and its provenance directly from PyPI:

```bash
pip install pypi-attestations

pypi-attestations verify pypi \
    --repository https://github.com/IKrysanov/airflow-pytest-operator \
    pypi:airflow_pytest_operator-X.Y.Z-py3-none-any.whl

pypi-attestations verify pypi \
    --repository https://github.com/IKrysanov/airflow-pytest-operator \
    pypi:airflow_pytest_operator-X.Y.Z.tar.gz
```

Replace `X.Y.Z` with the version you are installing.

> `pypi-attestations` is an experimentation-grade CLI per its own
> documentation; PyPI's upload-time check is the primary trust path.
> Future `pip` releases are expected to expose attestation verification
> natively.

### Path 2 — verify the GitHub Release artifact (Sigstore bundle)

Starting with version 0.3.1, each GitHub Release also ships the built
distributions plus an `.intoto.jsonl` Sigstore bundle that covers the same
bytes published to PyPI (both are produced from a single `build` job —
there is no parallel rebuild). This enables offline verification using the
[GitHub CLI](https://cli.github.com/) (`gh` 2.49+):

```bash
# Download the release assets (wheel, sdist, and the bundle):
gh release download vX.Y.Z \
    --repo IKrysanov/airflow-pytest-operator \
    --pattern "*.whl" --pattern "*.tar.gz" --pattern "*.intoto.jsonl"

# Online: verify against the GitHub attestations API
# (simplest; requires network access to api.github.com):
gh attestation verify airflow_pytest_operator-X.Y.Z-py3-none-any.whl \
    --repo IKrysanov/airflow-pytest-operator

# Offline: verify against the downloaded bundle
# (no API access required; the bundle is self-contained):
gh attestation verify airflow_pytest_operator-X.Y.Z-py3-none-any.whl \
    --repo IKrysanov/airflow-pytest-operator \
    --bundle airflow-pytest-operator-X.Y.Z.intoto.jsonl
```

A successful verification prints the workflow and run that produced the
artifact:

```
✓ Verification succeeded!
  Workflow: …/release.yml@refs/tags/vX.Y.Z
```

A failure means the file did not come from this workflow — **do not
install**.

Both paths confirm the same guarantee: the artifact came from this
GitHub repository, was produced by `release.yml` (the only configured
Trusted Publisher), and has not been modified since publication.

## Usage

```python
import pendulum
from airflow import DAG
from airflow_pytest_operator import PytestOperator

with DAG(
    dag_id="run_tests",
    start_date=pendulum.datetime(2024, 1, 1),
    schedule=None,
) as dag:
    smoke = PytestOperator(
        task_id="smoke_tests",
        test_path="/opt/airflow/tests",      # next to your dags/ folder
        pytest_args=["-k", "smoke", "-x"],   # any pytest CLI args
        env={"ENV": "staging"},              # extra env for the run
        fail_on_test_failure=True,           # task fails if any test fails
    )
```

The summary pushed to XCom (standard `return_value` key) looks like:

```python
{
    "total": 12, "passed": 11, "failed": 1, "skipped": 0, "errors": 0,
    "duration": 3.4, "exit_code": 1, "success": False,
    "failed_node_ids": ["tests.test_api::test_timeout"],
}
```

With `coverage=True` the summary additionally carries a `coverage` key — the overall coverage fraction in `[0, 1]`, or `None`; it is **absent** when coverage was not measured, so the shape above is unchanged by default. See [Coverage](#coverage-coverage).

The summary's shape is exported as a `TypedDict`, `RunSummary`, so you can type a downstream `xcom_pull` result (`from airflow_pytest_operator import RunSummary`). The block above is always present; `coverage`, `coverage_passed`, and the rerun keys (`rerun_rounds`, `recovered_node_ids`, `still_failing_node_ids`) are optional — read them with `.get(...)`. It is a plain `dict` at runtime.

`failed_node_ids` uses a dotted, parser-independent form. To feed them back
into a pytest run (a "retry only failed" task), convert them to CLI selectors
with `node_id_to_pytest_args`:

```python
from airflow_pytest_operator import node_id_to_pytest_args

def build_retry_args(**ctx):
    prev = ctx["ti"].xcom_pull(task_ids="run_tests") or {}
    # -> ["tests/test_api.py::test_timeout"]
    return node_id_to_pytest_args(prev.get("failed_node_ids") or [])
```

## Passing values from upstream tasks into your tests

A common pattern: an upstream task (say a `DataIngester`) creates rows in a
table and you want the test run to adapt — parametrise over the freshly
created IDs, target a specific table, etc. — **without editing the test
file** each time. The channel for this is the templated `env` field.

The important rule is **where** the Jinja template goes. Template the
**values inside the dict**, not the whole `env` as one string:

```python
# WRONG — templates the whole env as a single string. Jinja renders a dict
# to its *string repr* ("{'A': 'B'}"), so your test gets garbage.
env="{{ ti.xcom_pull(task_ids='ingest', key='cfg') }}"

# RIGHT — each value is templated independently and stays a clean string,
# which is exactly what an environment variable must be.
env={
    "TEST_IDS": "{{ ti.xcom_pull(task_ids='ingest', key='entity_ids') }}",
    "TARGET_TABLE": "{{ ti.xcom_pull(task_ids='ingest', key='target_table') }}",
}
```

Because environment variables are always strings, the upstream task should
push **already-serialised strings** to XCom (a CSV like `"101,102,103"` or a
JSON string), and the test parses them back. Full flow:

```python
# 1. Upstream task serialises what it produced into XCom as strings.
def ingest(**context):
    created_ids = [101, 102, 103]            # whatever the ingester made
    ti = context["ti"]
    ti.xcom_push(key="entity_ids", value=",".join(map(str, created_ids)))
    ti.xcom_push(key="target_table", value="fact_orders")

ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)

# 2. PytestOperator forwards those values via per-value templated env.
run_tests = PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    env={
        "TEST_IDS": "{{ ti.xcom_pull(task_ids='ingest', key='entity_ids') }}",
        "TARGET_TABLE": "{{ ti.xcom_pull(task_ids='ingest', key='target_table') }}",
    },
)

ingest_task >> run_tests
```

```python
# 3. The test reads the env var and parametrises over it. The function is
#    evaluated at collection time, by which point the operator has already
#    exported the variable, so the parametrisation sees the upstream values.
import os
import pytest

def _entity_ids():
    return [int(x) for x in os.environ.get("TEST_IDS", "").split(",") if x]

@pytest.mark.parametrize("entity_id", _entity_ids())
def test_entity_was_created(entity_id):
    table = os.environ["TARGET_TABLE"]
    # assert the row with entity_id exists in `table`
    ...
```

If you genuinely need a structured (non-string) object to survive
templating — e.g. you want `env` itself to come out as a dict from a single
XCom value — set `render_template_as_native_obj=True` on the DAG. Note that
this switches Jinja to native rendering for **every** templated field of
**every** task in that DAG, which can surprise other operators, so prefer
the per-value string approach above unless you specifically need native
objects.

## Constructor options

`PytestOperator` accepts the parameters below **plus every parameter that
[`BaseOperator`](https://airflow.apache.org/docs/apache-airflow/2.3.4/_api/airflow/models/baseoperator/index.html)
accepts** — `task_id`, `retries`, `execution_timeout`, `on_failure_callback`,
`trigger_rule`, `pool`, and so on. Airflow 3 users: `BaseOperator` moved to
`airflow.sdk`; the canonical reference is the
[Task SDK API docs](https://airflow.apache.org/docs/task-sdk/stable/api.html).

The parameters specific to `PytestOperator` are:

| Option | Default | Description |
|---|---|---|
| `test_path` | — | Target(s) passed to pytest: a file, directory, or node-id selector — or a sequence of them. Templated. |
| `pytest_args` | `[]` | Extra pytest CLI args, e.g. `["-k", "smoke", "-x"]`. Templated. See the [pytest reference](#pytest-config-plugins-and-allure) for what you can pass. |
| `markers` | `None` | Marker expression passed to pytest as `-m` (e.g. `"smoke and not slow"`). Discoverability sugar over `pytest_args`; templated; defers to an explicit `-m` in `pytest_args`; a value that renders empty is skipped. See [Selecting tests](#selecting-tests-markers--keyword). |
| `keyword` | `None` | Keyword expression passed to pytest as `-k` (e.g. `"login or logout"`). Same sugar/precedence/templating rules as `markers`. See [Selecting tests](#selecting-tests-markers--keyword). |
| `env` | `{}` | Extra environment variables for the run. Templated. |
| `env_file` | `None` | Path to a `.env` file merged into the test subprocess. Templated. The operator only forwards the path; the **runner** reads and merges it (precedence `os.environ` < `env_file` < `env`). Keys starting with `AIRFLOW` are skipped by default (see `env_file_overrides`). Needs the `[dotenv]` extra. See [.env files](#env-files). |
| `env_file_overrides` | `False` | When `False`, `env_file` can't override `AIRFLOW*` keys (so a `.env` can't break the worker's Airflow wiring in the child). `True` lifts that. The explicit `env` is never restricted. |
| `fail_on_test_failure` | `True` | Fail the task on any test failure/error. If `False`, the task always succeeds and the outcome is only reflected in XCom. |
| `dry_run` | `False` | Run pytest in `--collect-only` mode: import the test modules and walk the collection tree, but **do not execute test bodies**. Useful as a pre-flight task in a DAG; see [Dry-run mode](#dry-run-mode) below. |
| `test_retry_strategy` | `"all"` | How Airflow task **retries** re-run the suite. `"all"` re-runs everything; `"failed_only"` carries the previous attempt's failed node-ids in an Airflow Variable and re-runs **only those** on the next retry (deleted when no further retry will read it). See [Retry strategy](#retry-strategy-failed-only-reruns) below. |
| `store` | `VariableLastFailedStore()` | Backing store for the `failed_only` cross-retry set. Inject any object implementing the `LastFailedStore` protocol (`read`/`write`/`delete`) — a fake for tests or a custom backend; validated at init. Unused unless `test_retry_strategy="failed_only"`. |
| `rerun_failed` | `0` | **In-process** re-runs of only the failed tests, within one task. `N>0` runs the full suite then re-runs the still-failing tests up to `N` more times — no cache, no Airflow retry, robust on any executor. See [Retry strategy](#retry-strategy-failed-only-reruns). |
| `parallel` | `None` | Run the suite in parallel on the worker via `pytest-xdist` (`-n`). An int is the worker count; `"auto"`/`"logical"` map to xdist's CPU/logical-core counts. Applied to the first full run only (in-process `rerun_failed` rounds stay serial); ignored in `dry_run`; defers to an explicit `-n` in `pytest_args`. Needs the `[xdist]` extra on the worker. See [Parallel execution](#parallel-execution-parallel--dist). |
| `dist` | `None` | `pytest-xdist` scheduler mode (`--dist`): `"load"` (default behaviour, spread individual tests), `"loadscope"`/`"loadfile"`/`"loadgroup"` (pin a module-or-class/file/`xdist_group` to one worker), `"worksteal"`, `"each"`, `"no"`. Requires `parallel` to be set. See [Parallel execution](#parallel-execution-parallel--dist). |
| `coverage` | `False` | Measure coverage via `pytest-cov` on the first full run and push the overall fraction to XCom under the `coverage` key. Needs the `[coverage]` extra on the worker. See [Coverage](#coverage-coverage). |
| `cov_fail_under` | `None` | Coverage **gate**: a fraction in `[0, 1]` (e.g. `0.80`). Enables coverage automatically and fails the task with `CoverageThresholdError` when below the threshold (test failures take precedence; fail-closed if unmeasurable). See [Coverage](#coverage-coverage). |
| `do_xcom_push` | `True` | Airflow's standard flag. When on, the summary dict is pushed to XCom under the `return_value` key. Set `False` to disable all XCom output. Read it downstream with `xcom_pull(task_ids="<task>")`. |
| `runner` | `SubprocessPytestRunner()` | Injectable execution strategy (see *Extending*). |
| `parser` | `JUnitResultParser()` | Injectable report parser (see *Extending*). |

The default `SubprocessPytestRunner` additionally accepts `python_executable`, `timeout`, `cwd`, `grace_period`, `cleanup`, and `verbose` — see below. The **report location is set on the parser** (`JUnitResultParser(report_dir=...)`), not the runner — see [Report location & cleanup](#report-location--cleanup).

## .env files

Set `env_file` on the operator to load a batch of variables into the test run from a `.env`, without inlining each one in `env`:

```python
from airflow_pytest_operator import PytestOperator

PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    env_file="/opt/airflow/config/test.env",  # path is templated
    env={"RUN_ID": "{{ run_id }}"},            # explicit env still wins (see below)
)
```

`env_file` sits next to `env` on the operator, but the operator only forwards the path — the **runner** reads and merges the file, so the operator does no filesystem/`os` work and stays a thin orchestrator. Requires the `[dotenv]` extra (`pip install "airflow-pytest-operator[dotenv]"`); the file is parsed with `dotenv_values`, which **does not** mutate the worker's `os.environ`.

**Precedence** (lowest to highest, applied per key): `os.environ` → `env_file` → `env`. You can use `env` and `env_file` **together**: a key set in both takes its value from `env` (your explicit, per-task intent wins), a key only in the `.env` comes from the file, and a key only in `env` comes from `env`. The `.env` also overrides the inherited worker environment for ordinary (non-`AIRFLOW`) keys.

**`AIRFLOW*` keys are protected.** By default any key in the `.env` that starts with `AIRFLOW` (e.g. `AIRFLOW__CORE__...`, `AIRFLOW_HOME`, `AIRFLOW_CONN_*`) is **ignored**, so a stray `.env` can't clobber the worker's own Airflow wiring inside the child process and break test collection. Set `env_file_overrides=True` to lift that guard. The explicit `env` dict is never restricted — it's direct, per-task intent.

A missing `env_file`, or a missing `python-dotenv`, fails the task fast with a clear message (never a silent run without the file).

## Debugging a run (verbose diagnostics)

Set `verbose=True` on the runner to log the fully-resolved invocation to the task log right before pytest starts — handy when a run on a real worker doesn't behave as expected:

```python
from airflow_pytest_operator import PytestOperator
from airflow_pytest_operator.runners import SubprocessPytestRunner

PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    runner=SubprocessPytestRunner(verbose=True),
)
```

It logs the final `python -m pytest ...` command, the effective working directory, the **env delta against `os.environ`** (which keys this run adds vs overrides), and the report directory + cleanup policy. Credential-looking values are **masked** — keys matching `PASSWORD`/`TOKEN`/`SECRET`/`KEY`/`AUTH`/… and `AIRFLOW_CONN_*` (whose value is a connection URI with an embedded password) are printed as `***`, so secrets never reach the task log.

> **Don't put secrets in `pytest_args`.** The masking covers **env values only**. The pytest *command* — including your `pytest_args` — is logged verbatim, so a secret passed as a CLI flag (e.g. `--token=…`) would appear in the task log in clear text. Pass secrets via `env` / `env_file` instead (CLI args are also visible to anyone who can run `ps` on the worker).

## Streaming pytest output (`stream_output`)

By default (`stream_output=True`) pytest's stdout/stderr is logged to the task log **line-by-line as the suite runs**, so a long run isn't a blank screen until it finishes:

```python
PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    pytest_args=["-v"],   # one line per test streams cleanly; default progress dots are coarser over a pipe
)
```

The child runs unbuffered (`-u`) so lines flush promptly; stdout streams at `INFO`, stderr at `WARNING`. The full output is still captured in the result, and streamed lines share the runner's output cap (`max_output_bytes`, default 10 MiB), so a runaway suite can't flood the log.

Set `stream_output=False` to restore the old behaviour — one stdout/stderr blob logged once after the run. That's also **lighter on your logging backend**: one big record instead of thousands of small ones, which can matter for very chatty suites or per-record remote log handlers (Elasticsearch, Stackdriver, …).

## pytest config, plugins, and Allure

The operator runs real `python -m pytest`, so pytest discovers its own configuration (`pytest.ini`, `pyproject.toml`, `tox.ini`, `setup.cfg`) and `rootdir` exactly as on the command line. **Plugins and their options are picked up from your test folder's config automatically** — Allure, `pytest-xdist`, `pytest-cov`, markers, `addopts`, and so on. The operator only adds `--junitxml` (for its own parsing); everything else is yours.

> **pytest reference.** Everything you pass via `pytest_args` or a config file is plain pytest — its own docs are the quickest reference:
> - [How to invoke pytest (CLI)](https://docs.pytest.org/en/stable/how-to/usage.html) · [full flag reference](https://docs.pytest.org/en/stable/reference/reference.html#command-line-flags) · [configuration files](https://docs.pytest.org/en/stable/reference/customize.html)
> - [Markers (`-m`)](https://docs.pytest.org/en/stable/how-to/mark.html) · [Keyword selection (`-k`)](https://docs.pytest.org/en/stable/how-to/usage.html#specifying-which-tests-to-run) · [the cache (`--lf`, `--cache-clear`, `-p no:cacheprovider`)](https://docs.pytest.org/en/stable/how-to/cache.html)
> - Plugins: [pytest-cov](https://pytest-cov.readthedocs.io/) · [pytest-xdist](https://pytest-xdist.readthedocs.io/) · [coverage.py config](https://coverage.readthedocs.io/en/latest/config.html)

To make relative paths in `addopts` (e.g. `--alluredir=allure-results`) resolve next to your tests rather than the worker's working directory, the runner sets its working directory to the test folder by default: a directory target becomes the cwd, a file's parent becomes the cwd, and with multiple targets the closest shared parent is used. Node-id selectors (`path::test`) are anchored on their path portion — so this also applies to a `failed_only` retry, whose targets are all node-ids — and only a target with no resolvable path on disk falls back to the inherited cwd. Pass an explicit `cwd=` to override. Report paths stay absolute, so this never misplaces them.

```python
# pytest.ini next to your tests, with allure-pytest installed on the worker:
#   [pytest]
#   addopts = --alluredir=allure-results
# -> results land in <tests>/allure-results, as expected.
```

On distributed executors, make sure the plugins you reference (e.g. `allure-pytest`) are installed in the worker/pod environment, and write Allure output to persistent storage (volume/S3) rather than an ephemeral pod filesystem.

## Selecting tests (`markers` / `keyword`)

`markers` and `keyword` are ergonomic, discoverable shortcuts for pytest's `-m` and `-k` selectors — so a reader of the DAG sees *what* is being selected without decoding a `pytest_args` list:

```python
PytestOperator(
    task_id="api_smoke",
    test_path="tests/",
    markers="api and not slow",   # -> -m "api and not slow"
    keyword="login or logout",    # -> -k "login or logout"
)
```

Both are **templated**, so you can drive them from the DAG run (e.g. `markers="{{ dag_run.conf.get('markers', 'smoke') }}"`). They are spliced into the first full run (and they narrow `dry_run` collection too); they are equivalent to writing `-m`/`-k` in `pytest_args`, with two conveniences: if `pytest_args` already contains the flag the operator **defers to your explicit arg**, and a value that renders empty (e.g. a template that resolved to `""`) is **skipped** rather than passed as a blank selector. The in-process `rerun_failed` rounds re-run explicit node-ids, so the selectors are not re-applied there.

## Parallel execution (`parallel` / `dist`)

Run the suite in parallel **on a single worker** with [`pytest-xdist`](https://pypi.org/project/pytest-xdist/). Install the extra on the worker and set `parallel`:

```python
PytestOperator(
    task_id="tests",
    test_path="tests/",
    parallel=4,            # -> -n 4  (or "auto"/"logical" for CPU/logical cores)
    dist="loadscope",      # -> --dist loadscope (optional; needs `parallel`)
)
# pip install "airflow-pytest-operator[xdist]" on the worker
```

You could always pass `pytest_args=["-n", "4"]` by hand — these parameters add validation (a bad value fails at construction), keep parallelism off the in-process `rerun_failed` rounds (where spinning up workers for a couple of node-ids costs more than it saves), and read clearly in the DAG. If `pytest_args` already contains `-n`/`--numprocesses`, the operator defers to it entirely (and skips `dist`), so parallelism is never configured from both sides at once. Parallelism is skipped in `dry_run` (collection runs no test bodies).

**`dist` and the "everything ran on `gw0`" gotcha.** With `parallel` set, xdist's default scheduler is `load`, which spreads *individual tests* across workers. The `loadscope` / `loadfile` / `loadgroup` modes instead keep a whole **scope** — a module/class, a file, or an `xdist_group` — on one worker, so tests that share setup aren't split apart. The flip side: a suite whose tests **all live in one module/class** has a single scope, so under `loadscope`/`loadgroup` the entire run lands on `gw0` while the other workers sit idle. That is the mode working as designed, not a bug. To check what actually happened, read the xdist header in the task log: `4 workers [N items]` means four workers spawned (so any `gw0`-only run is a *distribution* effect — likely the scope above or a tiny/fast suite gw0 drained first), whereas `1 worker` means `-n` didn't take effect. Set `verbose=True` on the runner to log the fully-resolved pytest command.

> **Two axes of parallelism.** `parallel` scales *within* one worker (one Airflow task, N pytest processes). To spread a large suite *across* workers/pods, shard it with dynamic task mapping (below) — and each shard can still set `parallel` to use its own cores.

## Coverage (`coverage`)

Measure code coverage with [`pytest-cov`](https://pypi.org/project/pytest-cov/). Install the extra on the worker and set `coverage=True`:

```python
PytestOperator(
    task_id="tests",
    test_path="tests/",
    coverage=True,         # -> --cov --cov-report=term-missing on the first full run
)
# pip install "airflow-pytest-operator[coverage]" on the worker
```

This logs the coverage **table** to the task log and pushes the **overall line-coverage fraction** to XCom under the summary's `coverage` key — a float in `[0, 1]` (e.g. `0.85`), read from the table's `TOTAL` row, or `None` if no total could be parsed. The key is **absent** when coverage was not measured, so the default XCom shape is unchanged. Gate a downstream task on it:

```python
def coverage_gate(**ctx):
    cov = (ctx["ti"].xcom_pull(task_ids="tests") or {}).get("coverage")
    if cov is not None and cov < 0.80:
        raise ValueError(f"coverage {cov:.0%} below the 80% gate")
```

Or gate **natively**, without a separate task — set `cov_fail_under` (a fraction in `[0, 1]`):

```python
PytestOperator(task_id="tests", test_path="tests/", cov_fail_under=0.80)
```

This enables coverage measurement automatically (no need to also pass `coverage=True`) and **fails the task** with a clear `CoverageThresholdError` when the run is below the threshold; on pass the summary gains `coverage_passed=True`. Test failures take precedence (a red suite raises first), and it is **fail-closed** — a gate that can't be evaluated (no coverage total parsed) is an error, not a silent pass. Skipped in `dry_run`, deferred to an explicit `--no-cov`. A value outside `[0, 1]` is rejected — use `0.8`, not `80`. (This is the operator-level gate with a clear task-failure message; coverage.py's own `fail_under` below is the pytest-level equivalent.)

**Rules.** Applied to the first full run only (the in-process `rerun_failed` rounds run uncovered); skipped in `dry_run`; and deferred when `pytest_args` already drives `--cov`/`--no-cov` (a user-supplied `--cov` still surfaces a fraction if the run prints a terminal `TOTAL` row).

**Configuration** is coverage.py's own — set `source`, `omit`, report `precision`, and `fail_under` in `[tool.coverage.*]` (`pyproject.toml`) or `.coveragerc`; see the [coverage.py docs](https://coverage.readthedocs.io/en/latest/config.html). A configured `precision` is honoured (the XCom value is the number shown in the log), and `fail_under` makes the pytest run itself exit non-zero — failing the task independently of the XCom gate above.

> **What it measures.** `coverage.py` instruments the Python that runs **in the pytest worker process** — your code under `source`/`--cov`. For a **system/integration test that calls an external service** (REST, gRPC, a DB), it counts only the *local* client code, never the remote system's code (a different process/host). Such a test reports **low coverage of your package** by design — that is expected, not a regression. Point `[tool.coverage.run] source` at unit-testable packages, and don't hold a system-test task to the same threshold as a unit-test task. (To cover a remote Python service you must run coverage inside *that* process — see [subprocess measurement](https://coverage.readthedocs.io/en/latest/subprocess.html).)

## Sharding across workers (dynamic task mapping)

The **second axis**: split one suite *across* workers/pods, not just within one. The pattern — a `collect` task lists node-ids, they're split into N balanced groups, and one mapped `PytestOperator` runs per group:

```python
from airflow_pytest_operator import parse_collect_only_output, partition_node_ids
# collect --collect-only -> partition_node_ids(ids, N) -> .expand(test_path=groups)
```

Two public, pure helpers do the splitting (so they unit-test without Airflow):

- `parse_collect_only_output(stdout)` — pull node-ids out of `pytest --collect-only -q` output.
- `partition_node_ids(node_ids, num_shards)` — split them into up to `num_shards` balanced, **contiguous** groups (contiguous keeps a file's tests together, like `loadscope`; an empty group is never returned, so a shard can't accidentally run the whole suite).

Each shard can still set `parallel=` to xdist within its worker — the axes compose (`num_shards × cores`, so mind your pools/concurrency). With `test_retry_strategy="failed_only"` each shard retries only its own failures (the Variable key includes `map_index`). Full DAG: [`examples/sharded_mapped.py`](examples/sharded_mapped.py).

## Report location & cleanup

The **parser** owns where the report lands — set `report_dir` on it:

```python
from airflow_pytest_operator import JUnitResultParser, PytestOperator

PytestOperator(
    task_id="run_tests",
    test_path="/opt/airflow/tests",
    parser=JUnitResultParser(report_dir="/opt/airflow/artifacts"),  # your folder, kept
)
```

When `report_dir` is **not** set, the runner writes the report to a temporary
directory per run and cleans it up according to the `cleanup` policy on
`SubprocessPytestRunner`:

| `cleanup` | Behaviour |
|---|---|
| `"always"` (default) | Remove the temp dir after every run, including on test failure and on task kill/timeout. |
| `"on_success"` | Keep the temp dir when the run failed (for post-mortem); remove it on success. |
| `"never"` | Never remove it (e.g. you upload it as a CI artifact). |

A **parser-supplied** `report_dir` is your data and is never removed, regardless of policy. Cleanup also runs from `on_kill`, so killed tasks don't leak temp directories.

## Cancellation and timeouts

When Airflow kills the task (execution timeout, manual clear/mark-failed, worker shutdown), the operator's `on_kill` delegates to the runner, which terminates the **entire pytest process tree** — not just the direct child. This matters because pytest spawns its own children (e.g. `xdist` workers). Termination is graceful by default: `SIGTERM`, wait `grace_period` seconds (default 10), then `SIGKILL`. Set `timeout=` on the runner to bound the run itself. When that limit trips, the `TestExecutionError` carries the captured `stdout` / `stderr` as attributes, so you can inspect what the run printed before it hung.

> **Platform note:** process-group termination is fully supported on **Linux and macOS**. On Windows the package runs and cancels the direct process, but reliable whole-tree termination is not guaranteed; Airflow workers are Linux in virtually all deployments.

## Dry-run mode

`dry_run=True` runs pytest with `--collect-only`. Test bodies are not executed, but pytest still imports the test modules, walks the collection tree, and runs `conftest.py`. This is exactly what you want for a **pre-flight validation** task at the start of a DAG: it catches stale paths, missing deps on the worker, broken imports, `SyntaxError`s, and renamed fixtures in seconds rather than minutes.

```python
from airflow_pytest_operator import PytestOperator

# Pre-flight: validate that tests collect cleanly on this worker.
validate = PytestOperator(
    task_id="validate_tests",
    test_path="tests/",
    dry_run=True,
)

# Real run, gated on the pre-flight succeeding.
run = PytestOperator(
    task_id="run_tests",
    test_path="tests/",
)

validate >> run
```

A collection error (broken import, `SyntaxError`, missing fixture used by a parametrize) fails the dry-run task just like a test failure would, so downstream tasks are skipped and the real run never starts.

**What dry-run does and doesn't do:**

| Step | Real run | `dry_run=True` |
|---|---|---|
| Import test modules | yes | **yes** |
| Run module-level code | yes | **yes** |
| Run `conftest.py` | yes | **yes** |
| Collect tests (walk the tree) | yes | yes |
| Run collection-time fixtures | yes | yes |
| **Run test bodies** | yes | **no** |
| Run session/module/function teardown | yes | only for collection-time setup, if any |

So `dry_run` is not a no-op — module-level side effects happen. It's "collect only", which is meaningfully cheaper than a real run but still imports your code.

**Result interpretation:**

- With `parser=JSONResultParser()`, `TestRunResult.total` reports the number of collected tests (parsed from `summary.collected`).
- With the default `JUnitResultParser`, the XML pytest emits in `--collect-only` mode contains no `<testcase>` entries (`<testsuite tests="0">`), so `total` is `0`. The task still passes/fails correctly based on exit code; only the collected-count is unavailable. Use the JSON parser if you need the count for branching downstream.

**Interaction with user-supplied flags:** if you already passed `--collect-only` (or its aliases `--collectonly`, `--co`) in `pytest_args`, the operator won't add another one. The dedup is targeted only at the collect-only family — other repeated args (`-v -v`, multiple `-o KEY=VAL`, multiple `--ignore=...`) are preserved as-is.

## Retry strategy (failed-only reruns)

By default the **entire** pytest suite runs again whenever it is re-run. For a large suite where only a couple of tests failed, that wastes time. There are three ways to re-run only the failures — start with the first.

### Recommended: `rerun_failed` (built-in, any executor)

Set `rerun_failed=N` to make the operator **re-run only the failed tests, in-process, up to N times**, before reporting the final outcome — all within a single task:

```python
from airflow_pytest_operator import PytestOperator

run = PytestOperator(
    task_id="run_tests",
    test_path="tests/",
    rerun_failed=2,   # run all once, then re-run only the failures up to 2 more times
)
```

How it works:

- The operator runs the full suite once, then re-runs **only** the failed tests (converted via `node_id_to_pytest_args`), repeating up to `rerun_failed` times and stopping early as soon as none fail.
- It is **robust on any executor**: the set of failures is carried in memory across rounds within one `execute()`, so it needs **no metadata store, no Airflow Variable, no XCom between attempts, and no `try_number`** — none of the moving parts of the retry-driven approaches below.
- The task fails only if tests **still** fail after all rounds. The XCom summary keeps the first full run's counts and adds `rerun_rounds`, `recovered_node_ids`, and `still_failing_node_ids`. Ignored in `dry_run` mode.

This is the recommended way to absorb flaky failures and save compute. Use it alone, or combine it with Airflow's `retries=` — the in-process reruns happen first; if the task still fails, Airflow re-runs it from scratch on a fresh worker.

### Single-operator: `test_retry_strategy="failed_only"` (Airflow retries)

Set `test_retry_strategy="failed_only"` to make Airflow retries re-run **only the tests that failed on the previous attempt** — in a single task, driven purely by Airflow's own `retries=`:

```python
from datetime import timedelta

from airflow_pytest_operator import PytestOperator

run = PytestOperator(
    task_id="run_tests",
    test_path="tests/",
    retries=2,                          # Airflow's standard retry count
    retry_delay=timedelta(seconds=30),  # see note below — don't rely on the default
    test_retry_strategy="failed_only",  # retries re-run only what failed
)
```

> **Set a `retry_delay`.** `failed_only` only narrows on a *retry*, so you need
> at least two attempts to see it work. Airflow's **default `retry_delay` is 5
> minutes**, so without an explicit value the task sits in `up_for_retry` for
> five minutes between attempts — which is easy to mistake for the task being
> *hung*. It isn't; it's just waiting out the default delay. Pick a short
> `retry_delay` that suits your suite.

How it works:

- On the **first attempt of a fresh run** the store is empty, so the full suite runs; if it fails (and a retry remains) the failing node-ids are saved. Narrowing is driven purely by what's stored, **not** by `try_number` — so a *reused* `run_id` (a cleared/restarted run after a partial crash) may carry a prior set and narrow earlier.
- **A subsequent attempt** that finds a stored set reads it, converts it back to pytest selectors (via `node_id_to_pytest_args`), and passes them as the targets in place of `test_path` — so pytest collects and runs **only** the previously-failed tests. Your `pytest_args` are never mutated.

**Why an Airflow Variable, not XCom.** The failed set is carried between attempts in an **Airflow Variable** keyed by `(dag_id, task_id, run_id, map_index)` — `map_index` is included so the dynamically-mapped instances of one task (`.expand(...)`), which share a `run_id` and differ only by `map_index`, never clobber each other's failed set. A task **cannot** read its own XCom from a previous attempt — Airflow clears a task instance's XCom at the start of every retry — and writing a *different* task's XCom from inside a task is not portable to Airflow 3 (workers have no direct metadata-DB access). A Variable, by contrast, is readable **and** writable from within a task on both Airflow 2.x and 3.x, survives the task's own retries, and can be deleted when done. This unifies the old worker-local `--lf` cache trick and the two-task `run-all → run-failed` XCom pattern into one operator.

**Crash-safe cleanup (no orphaned Variables).** The Variable is **consumed on read**: a retry deletes it the instant it has read the failures and built its targets — *before* running a single test — so a worker killed mid-run (OOM/SIGKILL/pod eviction) can't leave one behind. A fresh copy is written at the **end** of an attempt **only when a further retry will read it** (the attempt failed *and* it is not the final one). On success, and on the final attempt, nothing is written, so the terminal attempt can never orphan a Variable even if it dies right before finishing. There is deliberately **no teardown-time delete a crash could skip** — the Variable exists only in the narrow gap between a failed non-final attempt and the retry that consumes it.

**Safe by construction.** If the Variable backend is unavailable, or the Airflow ids can't be derived from the context, the retry simply runs the **full suite** rather than failing — you never silently skip tests. Ignored in `dry_run` mode (there is no "last failed" to narrow to). The default `test_retry_strategy="all"` keeps the original behaviour (full suite on every retry).

> **Keep `fail_on_test_failure=True` (the default).** A retry only happens when a failing run actually *fails the task*, which it does only under `fail_on_test_failure=True`. With `fail_on_test_failure=False` the task always succeeds, Airflow never retries, and `failed_only` has nothing to narrow on — so the operator writes nothing forward (no orphaned Variable). If you want to inspect failures without failing the task, use the two-task `run-all → run-failed` pattern below instead.

> **Tip.** Want a different backing store (e.g. a custom KV store, or to scope the key differently)? Inject one: `PytestOperator(..., store=MyStore())`. Any object implementing the `LastFailedStore` protocol — `read(key) -> list[str]`, `write(key, ids)`, `delete(key)` — works (structural typing, so it type-checks under mypy without subclassing); the default is `VariableLastFailedStore`.

For absorbing flaky failures **without** an Airflow retry at all, prefer `rerun_failed` above (in-process, no metadata writes). For splitting the work across two explicit tasks, see the pattern below.

### Robust: run-all → run-failed (any executor)

For a result that does **not** depend on the worker cache, split the work across two tasks and carry the failed test ids through **XCom** (Airflow's metadata DB). Because the second task reads a *different* task's XCom, Airflow never clears it (it only clears a task's own XCom on that task's own retry) — so this works on any executor and survives a worker/pod dying between tasks:

```python
from airflow.decorators import task
from airflow_pytest_operator import PytestOperator, node_id_to_pytest_args
from airflow_pytest_operator.runners import SubprocessPytestRunner


def _failed(summary: dict | None) -> list[str]:
    # Read failed_node_ids out of the XCom summary and convert them to pytest
    # selectors; yields [] when nothing failed (so the short-circuit stays clean).
    return node_id_to_pytest_args((summary or {}).get("failed_node_ids") or [])


with DAG(...) as dag:
    # 1) Run everything; don't fail the task, so the summary (with
    #    failed_node_ids) is pushed to XCom for the next task.
    run_all = PytestOperator(
        task_id="run_all",
        test_path="/opt/airflow/tests",
        fail_on_test_failure=False,
    )

    # 2) Skip the rerun when nothing failed.
    @task.short_circuit
    def has_failures(summary: dict) -> bool:
        return bool(_failed(summary))

    # 3) Turn the XCom summary into pytest selectors for the failed tests.
    @task
    def to_selectors(summary: dict) -> list[str]:
        return _failed(summary)

    summary = run_all.output
    selectors = to_selectors(summary)

    # 4) Re-run ONLY the failed tests. cwd is the pytest rootdir (where
    #    pytest.ini / pyproject lives) so the relative selectors resolve.
    #    Give this task its own retries for several rounds if you like — they
    #    stay reliable because the ids live in run_all's XCom, not a cache.
    run_failed = PytestOperator(
        task_id="run_failed",
        test_path=selectors,
        fail_on_test_failure=True,
        retries=2,
        runner=SubprocessPytestRunner(cwd="/opt/airflow"),
    )

    run_all >> has_failures(summary) >> selectors >> run_failed
```

The `_failed` helper just reads `failed_node_ids` out of the XCom summary and runs them through `node_id_to_pytest_args`, returning `[]` when nothing failed. The snippet above is the complete pattern.

## Where do the tests live?

The operator runs whatever path exists **on the worker** at execute time, so it works with any executor (Local, Celery, Kubernetes, custom) — the runner spawns pytest wherever the task already runs. The practical constraint is *availability*: with `LocalExecutor` the tests sit next to `dags/`; with Celery/Kubernetes, make sure the test folder is synced to workers the same way DAGs are (git-sync, baked image, shared volume), or point `test_path` at wherever they land. If the path is missing, the task fails with a clear `TestExecutionError`.

## Built-in parsers

| Parser | Report format | Install requirement on the worker |
|---|---|---|
| `JUnitResultParser` (default) | JUnit XML (`--junitxml`) | nothing extra; pytest ships with it |
| `JSONResultParser` | pytest-json-report JSON | `pip install airflow-pytest-operator[json-report]` |

Both implement the same `ResultParser` interface and produce the same `TestRunResult`, so callers downstream of XCom don't care which one ran. Swap them via the `parser=` argument:

```python
from airflow_pytest_operator import JSONResultParser, PytestOperator

PytestOperator(
    task_id="t",
    test_path="tests/",
    parser=JSONResultParser(),
)
```

## Extending it

The operator depends on two narrow abstractions and accepts them via constructor injection — no operator subclassing required. Provide your own to change *how* tests run or *how* results are parsed.

The runner is **format-agnostic**: it does not know whether the report is JUnit XML, JSON, or anything else. Parsers declare the pytest CLI flags they need (and the path the report will land at) via `report_request(report_dir)`; the runner offers a temp `report_dir` as a fallback, but the parser owns the location and may use its own. Adding a new format means writing a new parser, not editing the runner.

### Custom parser

A parser implements two methods: `report_request` declares what pytest must produce, `parse` interprets the resulting file. The base `ResultParser` accepts `report_dir` — honor it (falling back to the runner's), and return an **absolute** path so it survives the runner's cwd handling.

```python
import os
from airflow_pytest_operator import (
    PytestOperator, ReportRequest, ResultParser, TestRunResult,
)

class TAPResultParser(ResultParser):
    """Example: read TAP (Test Anything Protocol) output via pytest-tap."""

    REPORT_FILENAME = "results.tap"

    def report_request(self, report_dir: str) -> ReportRequest:
        out_dir = os.path.abspath(self.report_dir or report_dir)
        path = os.path.join(out_dir, self.REPORT_FILENAME)
        return ReportRequest(
            pytest_args=("--tap-files", f"--tap-outdir={out_dir}"),
            report_path=path,
        )

    def parse(self, report_path: str, *, exit_code: int = 0) -> TestRunResult:
        ...  # read TAP, return a TestRunResult

# location set on the parser; runner uses a temp dir when omitted
PytestOperator(task_id="t", test_path="tests/", parser=TAPResultParser(report_dir="/artifacts"))
```

### Custom runner

```python
from airflow_pytest_operator import PytestOperator, PytestRunner, RunArtifacts

class DockerPytestRunner(PytestRunner):
    def run(
        self,
        test_path,
        *,
        pytest_args=None,
        env=None,
        env_file=None,             # forwarded from the operator; honor or raise
        env_file_overrides=False,
        report_request,            # required: parser-supplied callback
    ) -> RunArtifacts:
        report_dir = "/some/prepared/dir/in/the/container"
        spec = report_request(report_dir)
        # spawn pytest in a container with: [..., *spec.pytest_args, ...]
        # then collect the file from spec.report_path
        return RunArtifacts(exit_code=..., report_path=spec.report_path)

    # optional: override cancel() / cleanup() if you own resources
    # (the base class provides safe no-op defaults)

PytestOperator(task_id="t", test_path="tests/", runner=DockerPytestRunner())
```

## Architecture

| Concern | Type | Responsibility |
|---|---|---|
| `PytestOperator` | operator | orchestrate runner→parser, Airflow integration, fail/cleanup policy |
| `PytestRunner` / `SubprocessPytestRunner` | runner | execute pytest (format-agnostic), produce `RunArtifacts`, own cancel/cleanup |
| `ResultParser` / `JUnitResultParser` / `JSONResultParser` | parser | declare pytest's report args via `report_request`, then `parse` the resulting file into `TestRunResult` |
| `ReportRequest` | domain | what a parser asks pytest to produce (CLI flags + output path) |
| `compat.airflow` | shim | the only place that imports Airflow |
| `models` | domain | framework-free dataclasses |

## Development

The library's own tests run **without Airflow** by stubbing `BaseOperator` — itself a demonstration of the dependency-inversion design.

```bash
pip install -e ".[dev]"
ruff check src tests
mypy
pytest --cov
```

## Changelog

See [CHANGELOG](CHANGELOG.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
