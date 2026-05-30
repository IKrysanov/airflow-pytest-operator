# Security Policy

We take the security of `airflow-pytest-operator` seriously. This document
describes how to report vulnerabilities, which versions receive security
fixes, and the timelines you can expect.

## Supported versions

Security fixes are released only for the **current minor version**.
The project is pre-1.0; users on older minors are expected to upgrade to
the latest release to receive security fixes.

| Version | Status |
|---------|--------|
| 0.3.x   | ✅ Supported |
| < 0.3   | ❌ Not supported — please upgrade |

Once 1.0 is released this policy will be reviewed and tightened. For now,
treat any pre-1.0 release older than the latest minor as end-of-life from a
security standpoint.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub
issues, discussions, or pull requests.** Public reports give attackers a
window between disclosure and a fix landing in users' deployments.

Use **GitHub's Private Vulnerability Reporting** instead. From the
[Security tab of this repository](https://github.com/IKrysanov/airflow-pytest-operator/security)
click **"Report a vulnerability"**. This opens a private advisory visible
only to you and the maintainer; GitHub handles the notifications and
provides a workspace for coordinating the fix.

If for some reason you cannot use Private Vulnerability Reporting (for
example, you do not have a GitHub account and creating one is not an
option), you may open a regular GitHub issue containing **only** the words
*"please contact me about a security matter"* and your preferred contact
method — no technical details. The maintainer will reach out privately to
continue the conversation off-issue.

### What to include in a report

A good report contains, where applicable:

- A short summary of the issue and the impact you believe it has.
- Affected version(s) of `airflow-pytest-operator`.
- Affected Airflow version(s) and Python version(s), if relevant.
- Steps to reproduce, or a minimal proof of concept.
- Any suggested mitigation or fix, if you have one.

You do not need a CVE assigned to file a report — the maintainer will
request one if the issue warrants it.

## What to expect after reporting

- **Acknowledgement: within 72 hours** of receiving the report.
- **Initial assessment: within 7 days**, including whether the issue is
  confirmed, its preliminary severity, and rough remediation timeline.
- **Coordinated disclosure: 90 days** from initial report, extendable by
  mutual agreement if a fix requires more time. After 90 days, or after a
  fix is released (whichever is sooner), the advisory may be made public.
- **Credit:** reporters are credited in the GitHub Security Advisory and
  in `CHANGELOG.md` unless they prefer to remain anonymous.

These timelines reflect a single-maintainer project and are honest rather
than aspirational. If a report sits unacknowledged past 72 hours, treat
that as an outage — feel free to re-ping via the same channel.

## Out of scope

The following are **not** considered security issues by this project:

- Behaviour of pytest, test code, or third-party plugins invoked by the
  operator. Running arbitrary pytest code is the operator's purpose; the
  security boundary is whoever can write DAGs and place tests on a worker.
- Misconfiguration of the surrounding Airflow deployment (permissive
  Connections, weak worker isolation, lack of network segmentation).
- Vulnerabilities that require write access to the DAGs folder or the
  worker filesystem — those imply a compromised Airflow environment, which
  has its own security model.

If you are unsure whether something is in scope, **file a report anyway** —
we would rather review and close than miss a real issue.

## Hardening recommendations for users

A few things you can do today, in your own environment, to reduce risk:

- **Install with the hardened XML extra** when parsing JUnit reports from
  sources you do not fully trust:
  ```bash
  pip install "airflow-pytest-operator[secure-xml]"
  ```
  This pulls in `defusedxml` and routes the report parser through it,
  closing the standard XML attack classes (entity expansion, external
  entities, recursive expansion).
- **Verify release artifacts** before installing in sensitive environments;
  every release ships with [PEP 740](https://peps.python.org/pep-0740/)
  Sigstore attestations bound to this repository and workflow. See the
  *Verifying the release* section of the README for the exact commands.
- **Restrict who can write DAGs and tests** in your Airflow deployment to
  the same trust level as who can deploy production code — the operator
  executes whatever pytest is pointed at, so the trust boundary is the DAG
  author, not the operator itself.

## Acknowledgements

Reporters who have responsibly disclosed security issues will be listed
here. None to date.