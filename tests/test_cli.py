from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from parquet_drift.cli import main


def test_json_cli_pass_and_exit_code(tmp_path: Path, write_parquet, capsys) -> None:
    write_parquet(tmp_path, "a.parquet", pa.schema([("id", pa.int64())]))

    exit_code = main(["audit", str(tmp_path), "--format", "json", "--jobs", "1"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert json.loads(captured.out)["status"] == "pass"
    assert captured.err == ""


def test_text_cli_reports_drift_and_exit_one(tmp_path: Path, write_parquet, capsys) -> None:
    write_parquet(tmp_path, "a.parquet", pa.schema([("id", pa.int64())]))
    write_parquet(tmp_path, "b.parquet", pa.schema([("id", pa.string())]))

    exit_code = main(["audit", str(tmp_path), "--jobs", "1"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "DRIFT" in captured.out
    assert "type_changed" in captured.out
    assert captured.err == ""


def test_empty_dataset_is_usage_error(tmp_path: Path, capsys) -> None:
    exit_code = main(["audit", str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "no .parquet files" in captured.err


def test_invalid_jobs_is_usage_error(tmp_path: Path, capsys) -> None:
    exit_code = main(["audit", str(tmp_path), "--jobs", "0"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "--jobs must be at least 1" in captured.err
