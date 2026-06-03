# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-06-...
 
### Breaking changes
This release removes the runner's hardcoded knowledge of the JUnit format.
Parsers now declare which pytest CLI flags they need and where their report
will land; the runner just splices those args verbatim and reports back the
declared path. No deprecation aliases are provided -- breakage is intentional
and loud, because the silent-fallback alternative (a `JSONResultParser` that
secretly gets a JUnit XML file) is much harder to diagnose than a `TypeError`
at startup.
 
Migration matrix (was -> is):
 
- `RunArtifacts.junit_xml_path` -> `RunArtifacts.report_path`.
- `ResultParser` -- subclasses now MUST implement `report_request(report_dir)`
  in addition to `parse(...)`. A class that overrides only `parse` will raise
  `TypeError` at instantiation. The new method returns a `ReportRequest` with
  the CLI flags and the report path the parser wants pytest to produce.
- `PytestRunner.run(...)` -- new required keyword-only argument
  `report_request: Callable[[str], ReportRequest]`. Operators pass
  `parser.report_request`; custom runners receive the callback and must call
  it on the prepared report directory before launching pytest.
- `SubprocessPytestRunner` no longer adds `--junitxml=...` or
  `-o junit_logging=all` of its own accord. Those flags live in
  `JUnitResultParser.report_request` now.
- `TestExecutionError` raised on a missing report previously read
  `"pytest produced no JUnit report"`; it now names the configured parser
  class (`"pytest produced no JUnitResultParser report"`,
  `"... no JSONResultParser report"`, ...). Tests asserting against the old
  wording must update the match string.
### Added
- `ReportRequest` dataclass in `airflow_pytest_operator.models` (also
  re-exported from the package root). Frozen, with `pytest_args:
  tuple[str, ...]` and `report_path: str | None`. `report_path=None` is
  the documented signal for parsers that read stdout instead of a file
  (no built-in implementation in this release; the type permits it).
- `JSONResultParser` in `airflow_pytest_operator.reporters.json_parser`,
  parsing output produced by the `pytest-json-report` plugin. Same
  contract as `JUnitResultParser`: counts, durations, per-case results,
  and `failed_node_ids`. Available from the package root.
- `[json-report]` extra wiring `pytest-json-report>=1.5`. Install on
  workers configured to use the JSON parser:
      `pip install airflow-pytest-operator[json-report]`
  The parser itself has no runtime dependency on the plugin -- it just
  parses whatever JSON it is handed -- so this extra only needs to be
  on the side where pytest runs.
- The `[dev]` extra now also pulls in `pytest-json-report`, so
  `tests/test_json_parser.py` runs as part of the normal test suite.
- The error message produced when pytest fails to write a report now
  names the configured parser class (so logs say "no JUnitResultParser
  report" / "no JSONResultParser report" rather than the parser-agnostic
  "no report"), and truncates very long captured stderr at 4096 chars to
  keep Airflow task logs and XCom payloads bounded.
- Two new worker-oriented extras: `[pytest]` (`pytest>=7.0`) and
  `[pytest-allure]` (`pytest>=7.0, allure-pytest>=2.13`). These let workers
  pull in pytest (and optionally the Allure plugin) as part of a single
  `pip install airflow-pytest-operator[pytest]` command without manually
  tracking a separate requirement. The `[dev]` extra is unchanged and
  continues to include `pytest` alongside the development toolchain.
### Changed
- `SubprocessPytestRunner` is now format-agnostic. It receives a
  `report_request` callback from the operator, invokes it on the prepared
  report directory, splices the returned CLI args into the pytest command,
  and returns the declared report path in `RunArtifacts`. Adding a new
  report format is now strictly a matter of writing a new parser; the
  runner needs no changes. (This closes the gap between the OCP claim in
  the README and what the code actually allowed.)
- `SubprocessPytestRunner` no longer collects stdout/stderr via
  `communicate()`. Two background threads drain each pipe from the moment
  Popen returns, accumulating chunks until EOF. The main thread waits via
  `proc.wait(timeout=...)` and then joins the drainers with a bounded
  timeout. This removes a documented race: previously, the post-timeout
  tail was collected by a second `communicate()` call, which CPython
  documents as best-effort and which races SIGKILL against the kernel's
  pipe-flush -- on a saturated pipe the tail could come back empty even
  when bytes were waiting in the buffer. The new design captures every
  byte the child wrote before the kill, plus also covers the cancel()
  path that previously dropped the tail entirely.
- `JSONResultParser` now treats unknown `outcome` values as `"skipped"`
  instead of `"error"`. The previous default would flip a clean run to
  failed if a future pytest-json-report version introduced a new state
  (e.g. `"deselected"`, `"warned"`) and raise `TestsFailedError` on a
  suite that actually passed. `"skipped"` is non-fatal and honest: we did
  not classify the case as a real pass or failure. To prevent silent
  drift, the parser logs a single `WARNING` per report listing every
  unknown outcome it encountered, so schema changes still show up in
  worker logs rather than being papered over forever.
- `JSONResultParser` hardening pass:
  * Non-list `tests` field now raises `ReportParseError` instead of
    crashing with `TypeError` from a `for`-loop. Callers can catch a
    single exception type for "report is malformed".
  * Non-numeric values in `summary` counters are still coerced to 0 (to
    keep the parse going), but trigger a single `WARNING` per report
    listing every offending key. Silent structural errors no longer
    produce misleading zero counts.
  * Skipped-case message extraction now returns just the reason string
    instead of the repr of the `(filename, lineno, 'Skipped: reason')`
    tuple pytest-json-report stores in `longrepr`. Falls back to the raw
    text on schema drift. Uses `ast.literal_eval` so report content is
    never executed.
  * Per-parser docstrings now document that the report filename is fixed
    and that reusing the same `report_dir` overwrites prior reports --
    callers needing history retention must give the runner a fresh dir
    per run (the default temp-dir behavior already does this).
- `ReportRequest.report_path=None` no longer claims to be the channel for
  "stdout-reading parsers" -- no such parser ships in 0.4.0. Documented
  as "no report file expected", with the type kept permissive so a
  future format that doesn't produce a file wouldn't need a model
  change.
- The package's public surface (`from airflow_pytest_operator import ...`)
  gained `ReportRequest` and `JSONResultParser`. `__all__` updated in
  step.
- DCO check now skips automated bot commits (Dependabot, github-actions,
  etc.), identified by their `…[bot]@users.noreply.github.com` author
  email. Bots cannot run `git commit -s`, and their provenance comes from
  GitHub's bot identity rather than a DCO sign-off, so requiring one only
  blocked dependency-update PRs.
- CI and CodeQL workflows now trigger on `pull_request` only (plus the
  weekly schedule for CodeQL), not on `push: main`. Under branch protection
  every change reaches `main` through a PR, so the PR run is the
  authoritative gate; the post-merge `push` run re-tested identical code
  and roughly doubled CI usage per change. Added `concurrency` groups with
  `cancel-in-progress` to CI, CodeQL, and DCO so superseded runs on the
  same ref are cancelled rather than left to finish.

## [0.3.1] - 2026-05-31

### Security
- Completed SHA-pinning of GitHub Actions across **all** workflows. 0.3.0
  pinned `release.yml` and `testpypi.yml`; this release also pins `ci.yml`
  (`actions/checkout`, `actions/setup-python`, `codecov/codecov-action`)
  and `dco.yml` (`actions/checkout`). Closes the remaining Pinned-
  Dependencies findings from OpenSSF Scorecard for GitHub-owned and
  third-party actions.
- Added a CodeQL static-analysis workflow (`.github/workflows/codeql.yml`)
  running on push, pull request, and a weekly schedule, publishing results
  to the Security tab. Satisfies the Scorecard SAST check.
- Added a Dependabot configuration (`.github/dependabot.yml`) that keeps
  GitHub Actions (SHA pins plus their version comments) and Python dev
  dependencies current, so upstream security fixes surface as pull requests
  rather than sitting behind frozen hashes. Satisfies the Scorecard
  Dependency-Update-Tool check.
- `release.yml` now also signs and attaches the distributions to the
  GitHub Release. A single `build` job produces the artifacts; both the
  PyPI publish job and a new `attest-and-attach` job consume the SAME
  `dist` artifact, so the `*.intoto.jsonl` Sigstore attestation attached
  to the GitHub Release is a real attestation of the bytes uploaded to
  PyPI -- not a separate rebuild made just to satisfy supply-chain
  scanners.

### Documentation
- README: new "Passing values from upstream tasks into your tests" section
  documenting how to forward XCom values into a test run via per-value
  templated `env` (the template goes inside each dict value, not around the
  whole `env`), with a `DataIngester` → `parametrize` end-to-end example and
  a note on `render_template_as_native_obj`.

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

[Unreleased]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/IKrysanov/airflow-pytest-operator/releases/tag/v0.1.0
