# Contributing

Bug reports should include the command, Python and PyArrow versions, operating
system, and a minimal reproducible schema. Do not attach private datasets; an
empty Parquet file with the same schema is enough for schema-only problems.

For code changes:

1. Create a focused branch.
2. Add a regression test before changing behavior.
3. Run the local checks below.
4. Open a pull request that explains the user-visible behavior and limits.

```bash
python -m venv .venv
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest --cov=parquet_drift --cov-fail-under=85
python -m build
python scripts/smoke_wheel.py
```

Changes to JSON output must remain backward compatible within report version 1
or introduce a new report version and bundled JSON Schema.
