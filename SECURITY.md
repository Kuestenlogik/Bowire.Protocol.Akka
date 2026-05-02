# Security Policy

## Reporting a Vulnerability

If you've found a security issue in this Bowire plugin, please report it privately so we can fix it before it's discussed publicly.

**Email:** security@kuestenlogik.de

Please include:

- A description of the issue and the affected component
- Steps to reproduce (or a proof-of-concept)
- The plugin version (visible in `bowire plugin list --verbose`)
- Your assessment of impact

We aim to acknowledge reports within **2 business days** and to ship a fix or coordinated disclosure plan within **30 days** of triage.

Please **do not** open a public GitHub issue for security reports.

## Scope

In scope: this plugin's source code, the published NuGet package, and the wire-format handling it implements.

Out of scope: bugs in upstream dependencies (please report those upstream; we will track and consume the fix), and issues in the Bowire core (please report those at <https://github.com/Kuestenlogik/Bowire/security>).

## Supported versions

This plugin is currently pre-1.0; we support the latest released version. Once 1.0 ships, we will publish a security-support matrix here.
