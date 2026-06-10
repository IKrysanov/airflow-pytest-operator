# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Change history

- [0.4.2 — PytestOperator: dry-run / collect-only test collection mode](#042---2026-06-06)
- [0.4.1 — Immutable TestRunResult.cases and unified failed_node_ids format](#041---2026-06-06)
- [0.4.0 — Format-agnostic runner and safe stdout/stderr draining (output cap)](#040---2026-06-04)
- [0.3.1 — Security hardening and CI improvements (SHA‑pinning, CodeQL)](#031---2026-05-31)
- [0.3.0 — Coverage and CI integration (pytest‑cov, Codecov)](#030---2026-05-30)
- [0.2.1 — Airflow 3 compatibility: lazy imports and worker startup fix](#021---2026-05-24)
- [0.2.0 — XCom contract and run behavior changes (single return_value, do_xcom_push)](#020---2026-05-24)
- [0.1.0 — Initial release: operator, runner, parser, core functionality](#010---2026-05-23)

## [Unreleased]

### Added
- **Multiple test targets**: `PytestOperator(test_path=...)` and
  `PytestRunner.run` now accept a single string *or* a sequence of strings,
  all passed to pytest as positional selectors. With no explicit ``cwd``, the
  working directory is derived as the closest shared parent of the targets.
- **Parser-owned report location**: parsers accept ``report_dir``, e.g.
  ``JUnitResultParser(report_dir="/opt/airflow/artifacts")`` (also
  ``JSONResultParser``). The location travels with the parser, so it applies to
  any runner. When unset, the runner writes to a temp dir it cleans up.
- `node_id_to_pytest_args(node_ids, *, class_prefix="Test")` -- converts the
  dotted ``failed_node_ids`` (from XCom) back into pytest CLI selectors, for a
  "retry only failed" DAG pattern. Idempotent; leaves malformed/slash-form
  input untouched.
- `failed_selectors(summary, *, class_prefix="Test")` -- convenience wrapper
  over ``node_id_to_pytest_args`` that reads ``failed_node_ids`` out of a
  ``PytestOperator`` XCom summary and returns the pytest selectors, yielding
  ``[]`` for a missing/empty summary so the run-all -> run-failed branch can
  short-circuit cleanly.
- Task-log lines for the report directory, run outcome, and cleanup /
  cancellation decisions, so the report location and lifecycle are visible.
- On a pytest **timeout**, the raised `TestExecutionError` now carries the
  captured `stdout` / `stderr` as attributes (not just in the worker log), so
  operators and UIs can surface *why* a run hung programmatically.
- `PytestOperator(test_retry_strategy="failed_only")` -- new optional
  argument controlling how Airflow task *retries* re-run the suite. With
  the default ``"all"`` the whole suite re-runs on every retry (behaviour
  unchanged). With ``"failed_only"`` the operator appends pytest's ``--lf``
  (``--last-failed``) on retry attempts (``try_number > 1``), so only the
  tests that failed on the previous attempt run again -- a large saving on
  big suites where only a few tests fail. The first attempt always runs the
  full suite. ``--lf`` is backed by pytest's ``.pytest_cache``, so the
  narrowing only happens when that cache from the previous attempt is still
  readable on the worker; otherwise pytest safely falls back to the full
  suite. The user's ``pytest_args`` are not mutated -- the flag is appended
  to a per-call effective list at ``execute()`` time and is not double-added
  if ``--lf``/``--last-failed`` is already present. An invalid value raises
  ``ValueError``. Default ``"all"`` -- behaviour unchanged for existing tasks.
  ``--lf`` is best-effort: it relies on the worker's ``.pytest_cache`` (it
  degrades to a full run on a fresh worker, e.g. a retry that lands on a
  different K8s/Celery pod, and can race between parallel tasks that share a
  pytest rootdir). For a cache-independent guarantee on any executor, use the
  ``run_all`` -> ``run_failed`` DAG pattern below.
- **Robust "retry only failed" recipe** documented: a ``run_all`` ->
  ``run_failed`` DAG pattern that carries ``failed_node_ids`` through XCom and
  reruns only the failed tests via ``node_id_to_pytest_args``. Unlike ``--lf``
  it does not depend on the worker's ``.pytest_cache``, so it works on any
  executor and survives a worker/pod dying between tasks. See
  ``examples/retry_failed_dag_pattern.py`` and the README "Retry strategy".

### Changed
- **Breaking (pre-release):** `SubprocessPytestRunner` no longer takes a
  ``report_dir`` argument -- set the location on the parser instead
  (``JUnitResultParser(report_dir=X)``). The runner owns only the temp-dir
  fallback and its ``cleanup`` policy.
- Empty / whitespace-only test targets (e.g. a Jinja expression that rendered
  to ``""``) are dropped with a warning; a run with no usable target fails fast.

### Fixed
- Relative targets and a relative parser ``report_dir`` now resolve correctly
  under the runner's derived cwd. Previously a relative target could
  double-join (``"tests"`` -> ``"tests/tests"``, "file or directory not
  found"), and a relative report path was written where the runner did not
  look -- so the report went missing and the task went red even with
  ``fail_on_test_failure=False``. Targets and report paths are now absolutised.
- `cleanup()` is idempotent: the operator cleans up twice on a kill (from
  ``execute()`` and ``on_kill``); it no longer logs the decision twice.
- `_resolve_cwd` falls back gracefully (with a warning) when targets share no
  common anchor (e.g. different Windows drives) instead of raising.
- **JSON parser** edge cases: a skip raised from a fixture finalizer
  (`pytest.skip()` in teardown) keeps its reason; the plural `errors` summary
  key is counted (not only the singular `error`); and parametrized node ids
  whose value contains `::` (e.g. `test_param[a::b]`) are split correctly.
- **Process-tree termination** no longer leaks an `OSError` / `PermissionError`
  (child changed gid, or a cancel/timeout race) — it falls back to killing the
  direct child. The auto-created temp report dir is also removed if the parser's
  `report_request` callback raises, and `report_dir` ownership now resolves
  symlinks so a symlinked path isn't mistaken for outside the runner's temp dir.
- `SubprocessPytestRunner` validates its `timeout` (must be positive) and
  `grace_period` (must be non-negative) at construction instead of failing
  obscurely later.

## [0.4.2] - 2026-06-06

### Added
- `PytestOperator(dry_run=True)` -- new optional argument that switches
  the run to pytest's ``--collect-only`` mode. Test bodies do NOT
  execute; pytest only collects them (imports modules, runs collection-
  time fixtures, walks the test tree). Intended as a pre-flight task in
  a DAG: verifies the test path resolves on the worker, that imports
  succeed (so the worker has all required deps installed), and that
  collection itself succeeds (no syntax errors, no missing fixtures).
  Collection errors surface the same way normal test failures do --
  the task fails with ``TestsFailedError`` under the default
  ``fail_on_test_failure=True``. The user's ``pytest_args`` are not
  mutated; the ``--collect-only`` flag is appended to a per-call
  effective list at ``execute()`` time, so retries see the original
  configuration and downstream introspection of the operator is honest.
  Default ``dry_run=False`` -- behaviour unchanged for existing tasks.
- `JSONResultParser` now reports ``TestRunResult.total`` from
  ``summary.collected`` when ``summary.total`` is 0 and no per-case
  entries were parsed. This is exactly the shape pytest-json-report
  writes in ``--collect-only`` mode (zero "ran" tests, N collected).
  Normal runs are unaffected -- the fallback only kicks in when there
  is no other signal. The JUnit XML for ``--collect-only`` contains
  no ``<testcase>`` entries at all (``<testsuite tests="0">``), so the
  JUnit parser cannot report a count for dry-run; the operator
  docstring notes this limitation.

## [0.4.1] - 2026-06-06

### Fixed
- `TestRunResult.cases` is now a ``tuple[CaseResult, ...]`` instead of a
  ``list``, so the ``frozen=True`` claim on the dataclass is honest.
  Before this change, ``frozen=True`` only blocked attribute
  reassignment (``result.cases = [...]``); the list itself remained
  mutable so ``result.cases.append(...)`` silently modified a
  "frozen" instance. A ``__post_init__`` coerces any iterable the
  caller passes (list, generator, etc.) into a tuple, so the existing
  parsers that accumulate cases via ``.append`` on a local list and
  pass it through continue to work without change. **Breaking** only
  for external code that depended on the mutability of ``result.cases``
  -- which was the bug we're fixing.
- `JSONResultParser` and `JUnitResultParser` now produce identical
  ``failed_node_ids`` for the same suite. The JSON parser previously
  emitted pytest's native form (``"tests/test_x.py::test_y"``) while
  the JUnit parser emitted the dotted JUnit-XML form
  (``"tests.test_x::test_y"``), making downstream consumers (Airflow
  branches reading XCom, alerting that diffs the list across runs)
  silently parser-dependent. The JSON parser is now normalised to the
  same dotted form as JUnit. **Breaking** for any consumer that pinned
  on the slash form coming out of ``JSONResultParser`` since 0.4.0 --
  they get the new format starting with this release. The conversion
  direction was chosen for information-preservation: from the slash
  path with ``.py`` we can always derive the dotted module, but the
  reverse from JUnit's classname alone is ambiguous (``module.Class``
  vs ``module.subname``).
- `JUnitResultParser.parse` no longer catches ``Exception`` from the
  underlying XML parser. The handler now lists exactly the exception
  types that a JUnit parse can legitimately fail with:
  ``xml.etree.ElementTree.ParseError`` (malformed XML),
  ``ValueError`` (covers every ``defusedxml`` security exception via
  ``DefusedXmlException``), and ``OSError`` (file became unreadable
  between our ``exists()`` check and the actual read). Anything else
  -- ``MemoryError``, ``AttributeError`` from a bug in our code,
  ``RecursionError`` -- now escapes the parser uncaught so the worker
  logs the real problem instead of seeing a misleading
  "Failed to parse JUnit report" message.
- `JSONResultParser` now reports per-case ``time`` as the sum of the
  ``setup`` + ``call`` + ``teardown`` phase durations from
  ``pytest-json-report``, instead of reading only the ``call`` phase.
  Previously, a case that errored during ``setup`` (e.g. a fixture
  raised) had no ``call`` section in the JSON document and ended up
  with ``time=0.0`` -- making per-case timings misleading exactly for
  the failures users most want to investigate. The new behaviour
  matches what pytest's own JUnit XML writer reports as the case
  ``time`` and so restores parity with :class:`JUnitResultParser`.
  Malformed durations on individual phases are still tolerated: that
  phase contributes ``0`` and the others are summed as usual.
### Changed
- `SubprocessPytestRunner` drainer threads now count the per-stream
  output cap via ``len(chunk)`` (character count) instead of
  ``len(chunk.encode("utf-8", errors="replace"))``. The previous
  implementation allocated a throwaway ``bytes`` object on every
  ``readline()`` -- tens of thousands of allocations on a verbose
  suite -- just to get a precise byte count. ``len(chunk)`` is an
  O(1) cached lookup on ``str``. The trade-off is that the cap is now
  an *approximate* byte count: exact for ASCII output (which is what
  pytest emits in practically all cases), under-counting bytes by up
  to 4x for UTF-8 multi-byte content. The cap parameter is still
  named ``max_output_bytes`` for back-compat; the docstring
  documents the approximation. Microbench on a 10k-line ASCII/Cyrillic
  mix: 41ms -> 14ms per 50 iterations (~3x speedup) with a 1.002x
  under-count ratio.
- `TestRunResult.to_xcom` no longer goes through ``dataclasses.asdict``.
  The previous implementation recursively converted every nested
  :class:`CaseResult` into a dict tree before discarding the whole
  ``cases`` entry; for suites with thousands of tests that's a real
  amount of CPU and short-lived garbage on the worker. The new path
  builds the payload field-by-field, ~30x faster on a 5k-case suite
  (~227ms -> ~7ms in a microbench; bigger speedup on larger suites
  due to the per-case dataclass-to-dict overhead). The wire format is
  unchanged. A new structural test pins the set of XCom keys against
  the :class:`TestRunResult` schema so any future field addition is a
  conscious choice rather than a silent omission.

## [0.4.0] - 2026-06-04
 
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
  class (`"pytest produced no report for JUnitResultParser (exit code N)"`,
  `"pytest produced no report for JSONResultParser ..."`, ...). Tests
  asserting against the old wording must update the match string.
### Added
- `ReportRequest` dataclass in `airflow_pytest_operator.models` (also
  re-exported from the package root). Frozen, with `pytest_args:
  tuple[str, ...]` and `report_path: str | None`. `report_path=None`
  documents "no report file expected"; the type is kept permissive so a
  future format that produces no file needs no model change.
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
- The `TestExecutionError` raised when pytest writes no report truncates
  very long captured stderr at 4096 chars, keeping Airflow task logs and
  XCom payloads bounded. (The parser-class naming in that same message is
  covered under Breaking changes above.)
- `SubprocessPytestRunner` gained a `max_output_bytes` constructor
  parameter (default 10 MiB) that caps captured `stdout`/`stderr` per
  stream. A pytest run that writes unbounded output to a pipe (e.g.
  `-s` with a chatty or looping test) could otherwise grow the in-memory
  capture without limit and bloat the Airflow task log / XCom payload.
  Once a stream reaches the cap, further chunks from it are dropped and
  the captured text is suffixed with a one-line marker
  (`...(stdout truncated at N bytes; ...)`); the underlying pipe keeps
  being drained so the child never blocks on a full OS buffer. Pass
  `None` to restore unbounded capture; a non-positive value raises
  `ValueError`.
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

[Unreleased]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/IKrysanov/airflow-pytest-operator/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/IKrysanov/airflow-pytest-operator/releases/tag/v0.1.0
