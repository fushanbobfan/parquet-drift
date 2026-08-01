# Changelog

All notable changes to this project are documented here.

## 0.1.0 - 2026-07-31

- Scan every local Parquet footer recursively without reading row data.
- Group schema variants with deterministic SHA-256 fingerprints.
- Select the modal schema or an explicit golden Parquet reference.
- Report nested field, order, type, nullability, and optional metadata drift.
- Emit bounded human output and versioned, deterministic JSON for CI.
- Return distinct success, drift, and usage exit codes.
