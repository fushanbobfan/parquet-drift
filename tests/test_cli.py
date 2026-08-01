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
    assert "2 files, 2 schema variants, 0 unreadable files" in captured.out
    assert "(1 file, reference)" in captured.out
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


def test_json_cli_reports_drift_with_stable_keys(tmp_path: Path, write_parquet, capsys) -> None:
    write_parquet(tmp_path, "a.parquet", pa.schema([("id", pa.int64())]))
    write_parquet(tmp_path, "b.parquet", pa.schema([("id", pa.string())]))

    exit_code = main(["audit", str(tmp_path), "--format", "json", "--jobs", "1"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert list(json.loads(captured.out)) == sorted(json.loads(captured.out))


def test_argparse_help_returns_zero(capsys) -> None:
    exit_code = main(["--help"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "parquet-drift" in captured.out


def test_text_output_escapes_terminal_control_characters(
    tmp_path: Path, write_parquet, capsys
) -> None:
    write_parquet(tmp_path, "a.parquet", pa.schema([("bad\x1b[31m", pa.int64())]))
    write_parquet(tmp_path, "b.parquet", pa.schema([("other", pa.int64())]))

    exit_code = main(["audit", str(tmp_path), "--jobs", "1"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "\x1b" not in captured.out
    assert "\\u001b" in captured.out
