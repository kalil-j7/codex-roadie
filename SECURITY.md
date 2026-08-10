# Security policy

## Supported versions

Security fixes are applied to the latest commit on the default branch while Codex Roadie remains alpha software.

## Reporting a vulnerability

Do not open a public issue containing a vulnerability, credential, private repository path, recording, session identifier, or personal diagnostic log. Use GitHub's private vulnerability-reporting feature on this repository. If that feature is unavailable, open a minimal public issue asking the maintainer to enable a private reporting channel without including technical details.

## Execution warning

Roadie starts coding agents non-interactively. Workspace-write is the default. Full Mac access is an explicit opt-in that can read and modify anything available to the logged-in account. Review the exact repository and working directory before enabling it.

SSH support is intentionally key-only and non-interactive. Roadie must never be changed to store SSH passwords or embed API credentials.
