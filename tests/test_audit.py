from __future__ import annotations

import json
from importlib.resources import files
from pathlib import Path

import pyarrow as pa
import pytest
from jsonschema import validate

from parquet_drift import audit as audit_module
from parquet_drift.audit import InputError, audit_dataset


def test_audit_passes_for_one_schema_and_uses_relative_paths(tmp_path: Path, write_parquet) -> None:
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    write_parquet(tmp_path, "part=1/a.parquet", schema)
    write_parquet(tmp_path, "part=2/b.PARQUET", schema)

    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=2)

    assert report["status"] == "pass"
    assert report["summary"] == {
        "drifted_files": 0,
        "files": 2,
        "readable_files": 2,
        "schema_variants": 1,
        "unreadable_files": 0,
    }
    assert report["reference"]["strategy"] == "mode"
    assert report["groups"][0]["files"] == ["part=1/a.parquet", "part=2/b.PARQUET"]


def test_mode_reference_is_majority_not_first_file(tmp_path: Path, write_parquet) -> None:
    expected = pa.schema([("id", pa.int64())])
    outlier = pa.schema([("id", pa.string())])
    write_parquet(tmp_path, "a-outlier.parquet", outlier)
    write_parquet(tmp_path, "b.parquet", expected)
    write_parquet(tmp_path, "c.parquet", expected)

    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)

    assert report["status"] == "drift"
    assert report["reference"]["file_count"] == 2
    drift_group = next(group for group in report["groups"] if not group["is_reference"])
    assert drift_group["files"] == ["a-outlier.parquet"]
    assert drift_group["differences"] == [
        {
            "actual": "string",
            "code": "type_changed",
            "expected": "int64",
            "path": "/id",
        }
    ]


def test_explicit_reference_overrides_majority(tmp_path: Path, write_parquet) -> None:
    common = pa.schema([("id", pa.int64())])
    chosen = pa.schema([("id", pa.string())])
    reference = write_parquet(tmp_path, "chosen.parquet", chosen)
    write_parquet(tmp_path, "one.parquet", common)
    write_parquet(tmp_path, "two.parquet", common)

    report = audit_dataset(
        tmp_path,
        reference=reference,
        metadata="ignore",
        max_files_per_group=10,
        jobs=1,
    )

    assert report["reference"]["strategy"] == "explicit"
    assert report["reference"]["path"] == "chosen.parquet"
    assert report["summary"]["drifted_files"] == 2


def test_corrupt_file_is_a_data_failure(tmp_path: Path, write_parquet) -> None:
    write_parquet(tmp_path, "good.parquet", pa.schema([("id", pa.int64())]))
    (tmp_path / "broken.parquet").write_bytes(b"not parquet")

    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)

    assert report["status"] == "drift"
    assert report["summary"]["unreadable_files"] == 1
    assert report["unreadable"][0]["path"] == "broken.parquet"
    assert "D:" not in report["unreadable"][0]["message"]


def test_group_file_lists_are_bounded_but_counted(tmp_path: Path, write_parquet) -> None:
    schema = pa.schema([("id", pa.int64())])
    for index in range(5):
        write_parquet(tmp_path, f"{index}.parquet", schema)

    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=2, jobs=1)

    group = report["groups"][0]
    assert group["file_count"] == 5
    assert group["files"] == ["0.parquet", "1.parquet"]
    assert group["files_truncated"] == 3


def test_report_is_stable_across_worker_counts(tmp_path: Path, write_parquet) -> None:
    schema = pa.schema([("id", pa.int64())])
    for name in ["z.parquet", "a.parquet", "nested/m.parquet"]:
        write_parquet(tmp_path, name, schema)

    serial = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)
    parallel = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=3)

    assert serial == parallel


def test_file_input_and_external_reference_do_not_leak_absolute_paths(
    tmp_path: Path, write_parquet
) -> None:
    dataset = tmp_path / "dataset"
    reference_root = tmp_path / "reference"
    actual = write_parquet(dataset, "only.parquet", pa.schema([("id", pa.int64())]))
    reference = write_parquet(reference_root, "golden.parquet", pa.schema([("id", pa.string())]))

    report = audit_dataset(
        actual,
        reference=reference,
        metadata="ignore",
        max_files_per_group=10,
        jobs=1,
    )

    assert report["status"] == "drift"
    assert report["reference"]["path"] == "<external>/golden.parquet"
    assert report["groups"][0]["files"] == ["only.parquet"]
    assert str(tmp_path) not in str(report)


def test_mode_tie_is_visible_and_deterministic(tmp_path: Path, write_parquet) -> None:
    write_parquet(tmp_path, "a.parquet", pa.schema([("id", pa.int64())]))
    write_parquet(tmp_path, "b.parquet", pa.schema([("id", pa.string())]))

    first = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)
    second = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=2)

    assert first == second
    assert first["reference"]["selection_tied"] is True


def test_metadata_can_be_ignored_or_compared_exactly(tmp_path: Path, write_parquet) -> None:
    left = pa.schema([pa.field("id", pa.int64(), metadata={b"writer": b"one"})])
    right = pa.schema([pa.field("id", pa.int64(), metadata={b"writer": b"two"})])
    write_parquet(tmp_path, "a.parquet", left)
    write_parquet(tmp_path, "b.parquet", right)

    ignored = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)
    exact = audit_dataset(tmp_path, metadata="exact", max_files_per_group=10, jobs=1)

    assert ignored["status"] == "pass"
    assert exact["status"] == "drift"
    drift_group = next(group for group in exact["groups"] if not group["is_reference"])
    assert drift_group["differences"][0]["code"] == "field_metadata_changed"


def test_all_unreadable_files_still_return_a_stable_failure(tmp_path: Path) -> None:
    (tmp_path / "broken.parquet").write_bytes(b"broken")

    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)

    assert report["status"] == "drift"
    assert report["reference"] == {"strategy": "unavailable"}
    assert report["groups"] == []


def test_zero_file_limit_lists_all_paths(tmp_path: Path, write_parquet) -> None:
    schema = pa.schema([("id", pa.int64())])
    for index in range(3):
        write_parquet(tmp_path, f"{index}.parquet", schema)

    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=0, jobs=1)

    assert report["groups"][0]["files"] == ["0.parquet", "1.parquet", "2.parquet"]
    assert report["groups"][0]["files_truncated"] == 0


@pytest.mark.parametrize(
    ("path_kind", "message"),
    [
        ("missing", "path does not exist"),
        ("text", "file does not end in .parquet"),
    ],
)
def test_invalid_dataset_inputs_are_usage_errors(
    tmp_path: Path, path_kind: str, message: str
) -> None:
    path = tmp_path / ("missing" if path_kind == "missing" else "data.txt")
    if path_kind == "text":
        path.write_text("x", encoding="utf-8")

    with pytest.raises(InputError, match=message):
        audit_dataset(path, metadata="ignore", max_files_per_group=10, jobs=1)


def test_directory_traversal_errors_cannot_silently_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def denied_walk(root, *, topdown, onerror, followlinks):
        error = PermissionError(13, "permission denied", str(Path(root) / "blocked"))
        onerror(error)
        return []

    monkeypatch.setattr(audit_module.os, "walk", denied_walk)

    with pytest.raises(InputError, match="cannot traverse dataset directory blocked"):
        audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)


def test_symlinked_directories_are_rejected_instead_of_skipped(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "linked"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are not available")

    with pytest.raises(InputError, match="symlinked directory is not scanned: linked"):
        audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)


def test_invalid_limits_are_usage_errors(tmp_path: Path) -> None:
    with pytest.raises(InputError, match="--jobs"):
        audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=0)
    with pytest.raises(InputError, match="--max-files-per-group"):
        audit_dataset(tmp_path, metadata="ignore", max_files_per_group=-1, jobs=1)


def test_invalid_reference_is_a_usage_error(tmp_path: Path, write_parquet) -> None:
    write_parquet(tmp_path, "good.parquet", pa.schema([("id", pa.int64())]))
    broken = tmp_path / "broken.parquet"
    broken.write_bytes(b"broken")

    with pytest.raises(InputError, match="reference is not readable"):
        audit_dataset(
            tmp_path,
            reference=broken,
            metadata="ignore",
            max_files_per_group=10,
            jobs=1,
        )


def test_reports_validate_against_the_published_json_schema(tmp_path: Path, write_parquet) -> None:
    write_parquet(tmp_path, "a.parquet", pa.schema([("id", pa.int64())]))
    report = audit_dataset(tmp_path, metadata="ignore", max_files_per_group=10, jobs=1)
    schema_path = files("parquet_drift").joinpath("schemas/audit-report-v1.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    validate(instance=report, schema=schema)
