# parquet-contract

Catch schema drift across every Parquet shard before a reader or production
job does.

The project is intentionally narrow: it reads Parquet footers, stores a
versioned logical-schema baseline, and checks each file against that baseline.
It does not scan row values or replace a data quality platform.

Implementation is under active development.

