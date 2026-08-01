# parquet-drift

[![CI](https://github.com/fushanbobfan/parquet-drift/actions/workflows/ci.yml/badge.svg)](https://github.com/fushanbobfan/parquet-drift/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Find the Parquet shards whose schema differs before a reader fails—or quietly
uses an incomplete schema.

`parquet-drift` recursively reads every `.parquet` footer, fingerprints the
Arrow-visible logical schema, groups matching files, and explains each variant
field by field. It never scans row data.

## Why this exists

A partitioned dataset can contain thousands of files. One stale writer or one
bad export can introduce a different type, remove a column, or change nested
nullability. The hard part during an incident is usually locating the affected
files, not printing the schema of one file.

```console
$ parquet-drift audit data/ --max-files-per-group 1
DRIFT  20001 files, 2 schema variants, 0 unreadable files
Reference: mode sha256:9ec7...
Metadata: ignore
Schema sha256:9ec7... (20000 files, reference)
  year=2026/month=07/part-00000.parquet
  ... 19999 more files
Schema sha256:41a2... (1 file, drift)
  year=2026/month=07/part-17843.parquet
  - type_changed /amount: decimal128(18, 4) -> string
```

The default reference is the most common schema, not the first file returned by
the filesystem. Ties use a visible, deterministic fingerprint tie-break. For a
CI contract, pass a trusted reference file explicitly.

## Install

Python 3.10 or newer is required.

```bash
python -m pip install "parquet-drift @ git+https://github.com/fushanbobfan/parquet-drift.git@v0.1.0"
```

## Use

Audit a directory recursively:

```bash
parquet-drift audit data/
```

Use a checked-in golden file as the reference and emit machine-readable output:

```bash
parquet-drift audit build/dataset \
  --reference tests/golden/schema.parquet \
  --format json > parquet-drift-report.json
```

Compare arbitrary Arrow schema and field metadata byte-for-byte when it is part
of your contract:

```bash
parquet-drift audit data/ --metadata exact
```

By default, metadata is ignored because writer-specific keys such as pandas
metadata often change without changing the logical columns.

### Exit codes

| Code | Meaning |
| ---: | --- |
| `0` | Every readable shard matches the selected reference schema. |
| `1` | Schema drift or an unreadable Parquet file was found. |
| `2` | The path, reference, or command options are invalid. |

### JSON reports

JSON output is designed for CI and review:

- only POSIX-style paths relative to the audited dataset are emitted;
- files, groups, and differences have deterministic ordering;
- each schema variant has a SHA-256 fingerprint and a total file count;
- path lists are bounded by default; use `--max-files-per-group 0` to list all;
- no timestamp or machine-specific absolute root is included;
- `report_version` is stable and the bundled JSON Schema is versioned.

## What is compared

The comparison is recursive. It checks:

- field names and sibling order;
- nullability at every struct, list element, map key, and map value level;
- logical types and all type parameters, including timestamp unit/timezone and
  decimal width/precision/scale;
- struct, list, fixed-size-list, large-list, map, dictionary, union, and
  registered Arrow extension structure;
- schema and field metadata when `--metadata exact` is selected.

Paths use JSON Pointer escaping, so a real field named `a/b~c` is reported as
`/a~1b~0c`. List and map roles appear as `/[]`, `/{key}`, and `/{value}` rather
than relying on writer-specific physical child names.

The contract is deliberately the **Arrow-visible logical schema** returned by
PyArrow. Equivalent logical schemas may use different Parquet physical
encodings; this tool does not flag that as drift.

## Choosing the right tool

`parquet-drift` is an incident locator and CI gate, not a general data platform.

| Need | Better fit |
| --- | --- |
| Group every local shard and name the files/fields that drifted | `parquet-drift` |
| Build a full ODCS contract with quality rules and many data sources | [Data Contract CLI](https://docs.datacontract.com/) |
| Inspect one file's pages, row groups, or physical metadata | `parquet-tools` or another Parquet inspector |
| Merge schemas programmatically inside Python | `pyarrow.unify_schemas` |
| Enforce table schemas during writes and transactions | Delta Lake, Iceberg, or Hudi |

## Deliberate limits

Version 0.1 reads local files only. It does not:

- read values, profile columns, or run data quality rules;
- merge, repair, or rewrite files;
- validate Hive partition directory keys;
- compare Parquet field IDs, compression, encodings, or row-group layout;
- claim that a change is compatible with every Spark, Arrow, DuckDB, or Polars
  consumer;
- inspect S3, GCS, Azure, catalogs, Delta, Iceberg, or Hudi tables.

Directory audits reject symlinked directories and symlinked `.parquet` files
instead of silently following or skipping data outside the selected tree.

These limits keep the command fast, predictable, and useful alongside—not in
place of—the systems above.

## Development

```bash
python -m venv .venv
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest --cov=parquet_drift --cov-fail-under=85
python -m build
python scripts/smoke_wheel.py
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow and
[SECURITY.md](SECURITY.md) for security reporting. Released under the
[MIT License](LICENSE).
