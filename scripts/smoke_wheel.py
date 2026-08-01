"""Install the built wheel outside the checkout and exercise its console command."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import venv
from pathlib import Path


def run(arguments: list[str], *, cwd: Path | None = None, capture: bool = False):
    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONHOME", None)
    clean_environment.pop("PYTHONPATH", None)
    return subprocess.run(
        arguments,
        check=True,
        cwd=cwd,
        env=clean_environment,
        text=True,
        capture_output=capture,
    )


def venv_paths(root: Path) -> tuple[Path, Path]:
    if os.name == "nt":
        return root / "Scripts" / "python.exe", root / "Scripts" / "parquet-drift.exe"
    return root / "bin" / "python", root / "bin" / "parquet-drift"


def main() -> None:
    repository = Path(__file__).resolve().parents[1]
    wheels = sorted((repository / "dist").glob("parquet_drift-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected one wheel under dist/, found {len(wheels)}")

    with tempfile.TemporaryDirectory(prefix="parquet-drift-wheel-") as temporary:
        root = Path(temporary)
        environment = root / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python, command = venv_paths(environment)
        run([str(python), "-m", "pip", "install", str(wheels[0])], cwd=root)
        run([str(python), "-m", "pip", "check"], cwd=root)

        dataset = root / "dataset"
        dataset.mkdir()
        create_fixture = (
            "import pyarrow as pa; import pyarrow.parquet as pq; "
            f"from pathlib import Path; root=Path({json.dumps(str(dataset))}); "
            "table=pa.table({'id': pa.array([], type=pa.int64())}); "
            "pq.write_table(table, root/'a.parquet'); "
            "pq.write_table(table, root/'b.parquet')"
        )
        run([str(python), "-c", create_fixture], cwd=root)
        completed = run(
            [str(command), "audit", str(dataset), "--format", "json", "--jobs", "1"],
            cwd=root,
            capture=True,
        )
        report = json.loads(completed.stdout)
        if report["status"] != "pass" or report["summary"]["files"] != 2:
            raise SystemExit("installed command returned an unexpected audit report")
        version = run([str(command), "--version"], cwd=root, capture=True).stdout.strip()
        print(f"wheel smoke passed: {version}; {wheels[0].name}")


if __name__ == "__main__":
    main()
