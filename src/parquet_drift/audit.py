"""Scan every Parquet footer and group schema variants."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from parquet_drift.schema import MetadataMode, diff_schemas, fingerprint_schema, schema_to_model

REPORT_SCHEMA_URL = (
    "https://raw.githubusercontent.com/fushanbobfan/parquet-drift/"
    "v0.1.0/src/parquet_drift/schemas/audit-report-v1.schema.json"
)


class InputError(ValueError):
    """The requested audit cannot be configured safely."""


def audit_dataset(
    dataset: str | Path,
    *,
    reference: str | Path | None = None,
    metadata: MetadataMode = "ignore",
    max_files_per_group: int = 10,
    jobs: int = 4,
) -> dict[str, Any]:
    """Audit every local Parquet file and return a deterministic report."""
    if jobs < 1:
        raise InputError("--jobs must be at least 1")
    if max_files_per_group < 0:
        raise InputError("--max-files-per-group must be at least 0")

    root, files = _discover_files(Path(dataset))
    grouped: dict[str, dict[str, Any]] = {}
    readable_count = 0
    unreadable: list[dict[str, Any]] = []
    for result in _read_all(files, root=root, metadata=metadata, jobs=jobs):
        if "schema" not in result:
            unreadable.append(result)
            continue
        readable_count += 1
        schema = result.pop("schema")
        group = grouped.setdefault(
            result["fingerprint"],
            {"fingerprint": result["fingerprint"], "files": [], "schema": schema},
        )
        group["files"].append(result["path"])

    reference_model: dict[str, Any] | None
    reference_info: dict[str, Any]
    if reference is not None:
        reference_path = Path(reference).resolve()
        if not reference_path.is_file():
            raise InputError(f"reference is not a file: {reference}")
        reference_result = _read_one(reference_path, root=root, metadata=metadata)
        if "schema" not in reference_result:
            raise InputError(
                f"reference is not readable as Parquet: {reference_result['error']}: "
                f"{reference_result['message']}"
            )
        reference_model = reference_result["schema"]
        reference_fingerprint = reference_result["fingerprint"]
        reference_info = {
            "file_count": len(grouped.get(reference_fingerprint, {}).get("files", [])),
            "fingerprint": reference_fingerprint,
            "path": _display_reference_path(reference_path, root),
            "strategy": "explicit",
        }
    elif grouped:
        ranked = sorted(
            grouped.values(), key=lambda group: (-len(group["files"]), group["fingerprint"])
        )
        selected = ranked[0]
        top_count = len(selected["files"])
        reference_model = selected["schema"]
        reference_fingerprint = selected["fingerprint"]
        reference_info = {
            "file_count": top_count,
            "fingerprint": reference_fingerprint,
            "selection_tied": sum(len(group["files"]) == top_count for group in ranked) > 1,
            "strategy": "mode",
        }
    else:
        reference_model = None
        reference_fingerprint = None
        reference_info = {"strategy": "unavailable"}

    report_groups: list[dict[str, Any]] = []
    ordered_groups = sorted(
        grouped.values(),
        key=lambda group: (
            group["fingerprint"] != reference_fingerprint,
            -len(group["files"]),
            group["fingerprint"],
        ),
    )
    for group in ordered_groups:
        paths = sorted(group["files"])
        shown = paths if max_files_per_group == 0 else paths[:max_files_per_group]
        is_reference = group["fingerprint"] == reference_fingerprint
        report_group: dict[str, Any] = {
            "file_count": len(paths),
            "files": shown,
            "files_truncated": len(paths) - len(shown),
            "fingerprint": group["fingerprint"],
            "is_reference": is_reference,
        }
        if not is_reference and reference_model is not None:
            report_group["differences"] = diff_schemas(reference_model, group["schema"])
        report_groups.append(report_group)

    drifted_files = sum(
        len(group["files"])
        for group in grouped.values()
        if group["fingerprint"] != reference_fingerprint
    )
    status = "pass" if not unreadable and drifted_files == 0 and readable_count > 0 else "drift"
    return {
        "$schema": REPORT_SCHEMA_URL,
        "groups": report_groups,
        "metadata": metadata,
        "reference": reference_info,
        "report_version": 1,
        "status": status,
        "summary": {
            "drifted_files": drifted_files,
            "files": len(files),
            "readable_files": readable_count,
            "schema_variants": len(grouped),
            "unreadable_files": len(unreadable),
        },
        "unreadable": [
            {"error": result["error"], "message": result["message"], "path": result["path"]}
            for result in unreadable
        ],
    }


def _discover_files(dataset: Path) -> tuple[Path, list[Path]]:
    resolved = dataset.resolve()
    if not resolved.exists():
        raise InputError(f"path does not exist: {dataset}")
    if resolved.is_file():
        if resolved.suffix.lower() != ".parquet":
            raise InputError(f"file does not end in .parquet: {dataset}")
        return resolved.parent, [resolved]
    if not resolved.is_dir():
        raise InputError(f"path is not a file or directory: {dataset}")

    files: list[Path] = []

    def traversal_error(error: OSError) -> None:
        location = Path(error.filename) if error.filename else resolved
        relative = _relative_path(location, resolved)
        reason = error.strerror or type(error).__name__
        raise InputError(f"cannot traverse dataset directory {relative}: {reason}") from error

    for directory, names, filenames in os.walk(
        resolved,
        topdown=True,
        onerror=traversal_error,
        followlinks=False,
    ):
        names.sort()
        filenames.sort()
        current = Path(directory)
        for name in names:
            child = current / name
            try:
                is_symlink = child.is_symlink()
            except OSError as error:
                traversal_error(error)
            if is_symlink:
                relative = child.relative_to(resolved).as_posix()
                raise InputError(f"symlinked directory is not scanned: {relative}")
        for name in filenames:
            path = current / name
            if path.suffix.lower() == ".parquet":
                try:
                    is_symlink = path.is_symlink()
                    is_file = path.is_file()
                except OSError as error:
                    traversal_error(error)
                relative = path.relative_to(resolved).as_posix()
                if is_symlink:
                    raise InputError(f"symlinked Parquet file is not scanned: {relative}")
                if not is_file:
                    raise InputError(f"Parquet path is not a regular file: {relative}")
                files.append(path)
    if not files:
        raise InputError(f"no .parquet files found under: {dataset}")
    return resolved, files


def _read_all(
    files: list[Path],
    *,
    root: Path,
    metadata: MetadataMode,
    jobs: int,
) -> Iterator[dict[str, Any]]:
    if jobs == 1:
        for path in files:
            yield _read_one(path, root=root, metadata=metadata)
        return
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        yield from executor.map(lambda path: _read_one(path, root=root, metadata=metadata), files)


def _read_one(path: Path, *, root: Path, metadata: MetadataMode) -> dict[str, Any]:
    relative = _relative_path(path, root)
    try:
        model = schema_to_model(pq.read_schema(path), metadata=metadata)
    except Exception as error:  # PyArrow exposes several parser-specific exception classes.
        message = _sanitize_message(str(error), path=path, relative=relative)
        return {"error": type(error).__name__, "message": message, "path": relative}
    return {
        "fingerprint": fingerprint_schema(model),
        "path": relative,
        "schema": model,
    }


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.name


def _display_reference_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return f"<external>/{path.name}"


def _sanitize_message(message: str, *, path: Path, relative: str) -> str:
    sanitized = message.replace(str(path), relative).replace(path.as_posix(), relative)
    return " ".join(sanitized.split())
