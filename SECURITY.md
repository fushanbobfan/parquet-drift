# Security policy

## Supported versions

Security fixes are provided for the latest released version.

## Reporting a vulnerability

Use the repository's private **Report a vulnerability** form under the Security
tab. Please do not open a public issue for an undisclosed vulnerability.

Include the affected version, operating system, PyArrow version, impact, and a
minimal reproducer that does not contain private data. You can expect an initial
response within seven days.

## Trust boundary

The command does not execute shell commands, read row values, or use the
network. It does pass each selected file to PyArrow's Parquet footer parser.
Treat files from untrusted sources with the same caution as any input processed
by a native file-format parser, and keep PyArrow current.

Reports contain relative file paths and schema details. Field names and Arrow
metadata can still contain sensitive information; review JSON output before
publishing it.
