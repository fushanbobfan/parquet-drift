from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def write_parquet():
    def write(root: Path, relative: str, schema: pa.Schema) -> Path:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        arrays = [pa.array([], type=field.type) for field in schema]
        table = pa.Table.from_arrays(arrays, schema=schema)
        pq.write_table(table, path)
        return path

    return write
