"""Command-line interface for Parquet schema drift audits."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from typing import Any

from parquet_drift import __version__
from parquet_drift.audit import InputError, audit_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parquet-drift",
        description="Locate schema drift across every shard in a local Parquet dataset.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser(
        "audit", help="scan all Parquet footers and group schema variants"
    )
    audit.add_argument("dataset", help="a .parquet file or a directory scanned recursively")
    audit.add_argument(
        "--reference",
        help="trusted Parquet file to use as the reference schema (default: modal schema)",
    )
    audit.add_argument(
        "--metadata",
        choices=("ignore", "exact"),
        default="ignore",
        help="ignore arbitrary Arrow metadata or compare it byte-for-byte (default: ignore)",
    )
    audit.add_argument("--format", choices=("text", "json"), default="text")
    audit.add_argument(
        "--max-files-per-group",
        type=int,
        default=10,
        help="paths shown per schema group; 0 lists all paths (default: 10)",
    )
    audit.add_argument(
        "--jobs",
        type=int,
        default=min(8, max(1, os.cpu_count() or 1)),
        help="parallel footer reads (default: up to 8)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return int(error.code)

    try:
        report = audit_dataset(
            args.dataset,
            reference=args.reference,
            metadata=args.metadata,
            max_files_per_group=args.max_files_per_group,
            jobs=args.jobs,
        )
    except InputError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(_render_text(report))
    return 0 if report["status"] == "pass" else 1


def entrypoint() -> None:
    raise SystemExit(main())


def _render_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    heading = "PASS" if report["status"] == "pass" else "DRIFT"
    lines = [
        (
            f"{heading}  {summary['files']} files, {summary['schema_variants']} schema variants, "
            f"{summary['unreadable_files']} unreadable"
        )
    ]
    reference = report["reference"]
    if reference["strategy"] != "unavailable":
        detail = f"Reference: {reference['strategy']} {reference['fingerprint']}"
        if reference.get("path"):
            detail += f" ({reference['path']})"
        if reference.get("selection_tied"):
            detail += " [deterministic tie-break]"
        lines.append(detail)
    lines.append(f"Metadata: {report['metadata']}")

    for item in report["unreadable"]:
        lines.append(f"Unreadable {item['path']}: {item['error']}: {item['message']}")

    for group in report["groups"]:
        marker = "reference" if group["is_reference"] else "drift"
        lines.append(f"Schema {group['fingerprint']} ({group['file_count']} files, {marker})")
        for path in group["files"]:
            lines.append(f"  {path}")
        if group["files_truncated"]:
            lines.append(f"  ... {group['files_truncated']} more files")
        for difference in group.get("differences", []):
            lines.append(
                f"  - {difference['code']} {difference['path']}: "
                f"{difference['expected']} -> {difference['actual']}"
            )
    return "\n".join(lines)
