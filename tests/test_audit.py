from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from parquet_drift.audit import audit_dataset


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
