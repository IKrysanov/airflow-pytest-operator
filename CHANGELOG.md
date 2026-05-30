# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-05-30

### Added
- Coverage measurement wired into the project: `pytest-cov` is now a dev
  dependency and `[tool.coverage]` configuration lives in `pyproject.toml`
  (branch coverage on, `airflow_pytest_operator` as the source). Coverage is
  opt-in via `pytest --cov=airflow_pytest_operator` so the integration CI
  job, which installs a bare pytest, is unaffected.
- Codecov integration in CI: the unit job uploads a coverage report
  (`coverage.xml`) on Python 3.12 via `codecov/codecov-action`, and the
  README now carries a coverage badge.
- The CI integration matrix now also runs against Airflow 3.2.1 (py3.12),
  the release where the 0.2.1 provider-discovery startup crash first
  appeared, guarding the lazy-import fix against regression.
- Substantial test additions bringing measured coverage to ~99.5% on the
  test stub and on real Airflow 2.10.3, 3.0.6, and 3.2.1. The single
  uncovered line is a `run()`/`cancel()` race-guard with no deterministic
  test, left uncovered by design rather than chased with a flaky timing
  test (a `fail_under = 85` gate guards against real regressions):
  - `tests/test_models.py` — node-id reconstruction (with and without a
    classname), `success`/`failed_node_ids` derivation, and the XCom
    projection that drops per-case detail.
  - `tests/test_base_interfaces.py` — the abstract `PytestRunner`/
    `ResultParser` defaults (no-op `cancel`/`cleanup`, abstract-method
    contracts), proving Liskov-substitutability of minimal implementations.
  - `tests/test_compat.py` — every `BaseOperator` resolution branch of the
    compatibility shim, driven deterministically by injecting fake modules
    into `sys.modules`, plus `get_airflow_version` parsing and the
    `apply_defaults` passthrough.
  - Operator logging of child stdout/stderr and failed node ids, asserted
    by spying on the logger's methods rather than `caplog` (robust across
    Airflow versions, which route task logging differently).
  - `SubprocessPytestRunner` edge paths: `_terminate` when the process is
    already dead or disappears mid-signal (`ProcessLookupError` on SIGTERM
    and SIGKILL) and the cleanup race-guard that prevents a double `rmtree`.
  - A malformed `time` attribute in a JUnit report now has an explicit test
    confirming it degrades to `0.0` rather than failing the parse.

### Changed
- License headers on all source files now use a collective copyright
  ("the airflow-pytest-operator contributors") instead of an individual
  name, so contributors never have to edit copyright lines per file. A
  `NOTICE` file records project-level authorship, and each contributor
  retains copyright over their own contributions.
- OS-specific Windows-only branches in `SubprocessPytestRunner` are marked
  `# pragma: no cover`; they cannot execute on the Linux CI runners and are
  excluded from the coverage measurement rather than left as phantom gaps.

### Security
- A `SECURITY.md` security policy documents supported versions, the
  preferred reporting channel (GitHub Private Vulnerability Reporting),
  response-time expectations (acknowledgement within 72 hours, initial
  assessment within 7 days, 90-day coordinated disclosure), out-of-scope
  cases, and hardening recommendations for users (including the
  `[secure-xml]` extra and how to verify release attestations). Closes the
  Security-Policy criterion in supply-chain audits such as OpenSSF
  Scorecard.
- A weekly OpenSSF Scorecard analysis (`.github/workflows/scorecard.yml`)
  scans the repository for supply-chain best practices and publishes its
  result as a signed SARIF report to the GitHub Security tab and to the
  public Scorecard API. The score badge is linked from the README so
  consumers can see the project's supply-chain posture at a glance.
- Release and TestPyPI workflows now pin every third-party GitHub Action by
  immutable commit SHA (with a trailing comment naming the version) rather
  than by floating tag, so a compromise of an upstream action repository
  cannot silently substitute new code into the workflow that holds our
  PyPI OIDC token.
- Release artifacts ship with [PEP 740](https://peps.python.org/pep-0740/)
  Sigstore attestations, produced automatically by
  `pypa/gh-action-pypi-publish` v1.10+ for any Trusted-Publishing release.
  PyPI verifies them at upload and surfaces the source repository in the
  release's *Verified details*. README documents how end users can verify
  individual artifacts against this repository with `pypi-attestations`.

### Contributor experience
- `CONTRIBUTING.md` now documents the license header to copy into new files,
  the project's GitHub Flow branching model (PRs target `main`; there is no
  `develop` branch), a Developer Certificate of Origin (DCO) sign-off
  workflow, and a maintainer review/merge checklist.
- Added a `DCO` GitHub Actions workflow that verifies every pull-request
  commit carries a `Signed-off-by` trailer.

## [0.2.1] - 2026-05-24

### Fixed
- Prevent an Airflow worker startup crash on Airflow 3.2.x. Provider
  discovery imports this package during Airflow's own config
  initialization, before the Task SDK is ready; eagerly importing the
  operator (and thus `airflow.sdk.bases.operator`) at that point raised
  `ImportError: cannot import name 'conf' from 'airflow.sdk.configuration'`
  and aborted startup. Two changes break the chain: the provider-discovery
  entry point now lives in an import-light module
  (`airflow_pytest_operator.provider_info`), and `PytestOperator` is
  exposed lazily via module `__getattr__`, so importing the package no
  longer triggers the Airflow import chain. `_import_base_operator` also
  now raises a single diagnostic `ImportError` listing all attempted paths
  instead of leaking Airflow's internal deprecation traceback.

## [0.2.0] - 2026-05-24

### Fixed
- Resolve `BaseOperator` from `airflow.sdk.bases.operator` on Airflow 3,
  eliminating the `DeprecatedImportWarning` that came from the legacy
  `airflow.models.baseoperator` path. Import resolution now prefers the
  canonical Task SDK location and only falls back to the Airflow 2 path on
  Airflow 2.
- Reject concurrent `run()` calls on a single `SubprocessPytestRunner`
  instance (fail-fast with `TestExecutionError`) to prevent a race on the
  per-run temporary directory and process handle. Separate runner instances
  remain fully independent, and the same instance can be reused for
  sequential runs (e.g. task retries).
- `cancel()` no longer holds the internal lock during its graceful
  termination wait (up to `grace_period` seconds). The lock is now held only
  to snapshot the process handle, so `on_kill` no longer serializes the
  run's teardown or `cleanup()`.
- Wrap report-directory preparation so a user-supplied `report_dir` pointing
  at a file (or an unwritable location) raises `TestExecutionError` per the
  runner contract, instead of leaking a bare `OSError`.

### Changed
- **Breaking:** the test summary is now exposed only via the standard XCom
  `return_value` key. The previously-duplicated custom `pytest_result` key
  has been removed. Downstream tasks that read `xcom_pull(key="pytest_result")`
  must switch to `xcom_pull(task_ids="<task>")` (the default `return_value`).
- **Breaking:** removed the custom `push_result` parameter. XCom output is
  now controlled solely by Airflow's standard `do_xcom_push` (default `True`);
  pass `do_xcom_push=False` to disable. Replace `push_result=False` with
  `do_xcom_push=False`.

### Added
- Documentation on installing the package in Docker/constrained environments
  using Airflow's official constraint files.

## [0.1.0] - 2026-05-23

Initial release.

### Added
- `PytestOperator` — runs a pytest suite as an Airflow task, parses the JUnit
  report into a structured result, optionally pushes a summary to XCom under the
  `pytest_result` key, and fails the task on test failure (configurable via
  `fail_on_test_failure`).
- `PytestRunner` abstraction with a default `SubprocessPytestRunner` that runs
  `python -m pytest` in a child process using the worker's own interpreter and
  virtualenv.
- `ResultParser` abstraction with a default `JUnitResultParser` (uses
  `defusedxml` when available via the `secure-xml` extra).
- Airflow 2.x / 3.x compatibility through a single `compat.airflow` shim.
- Graceful cancellation: `on_kill` terminates the whole pytest process tree
  (`SIGTERM` → `grace_period` → `SIGKILL`), covering `xdist` workers and nested
  processes.
- Automatic working-directory resolution so relative paths in pytest `addopts`
  (e.g. Allure's `--alluredir`) resolve next to the tests.
- Temporary report-directory cleanup with `cleanup` policy
  (`"always"` | `"on_success"` | `"never"`); user-supplied `report_dir` is never
  removed; cleanup also runs on task kill.
- `run` timeout support and clear `TestExecutionError` when pytest produces no
  report (collection error, crash, wrong path).
- `push_result=False` suppresses all XCom output, including Airflow's automatic
  return-value push.
- Packaged as an Airflow provider (`get_provider_info` entry point), Apache-2.0
  licensed.

[Unreleased]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/IKrysanov/airflow-pytest-operator/releases/tag/v0.1.0
