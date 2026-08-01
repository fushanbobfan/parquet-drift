from __future__ import annotations

import pyarrow as pa

from parquet_drift.schema import fingerprint_schema, schema_to_model


def test_schema_model_preserves_nested_logical_details() -> None:
    schema = pa.schema(
        [
            pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("amount", pa.decimal128(18, 4)),
            pa.field(
                "payload",
                pa.struct(
                    [
                        pa.field(
                            "items",
                            pa.list_(pa.field("element", pa.int32(), nullable=False)),
                        )
                    ]
                ),
            ),
        ]
    )

    model = schema_to_model(schema, metadata="ignore")

    assert model["fields"][0] == {
        "name": "event_time",
        "nullable": False,
        "type": {"kind": "scalar", "name": "timestamp[us, tz=UTC]"},
    }
    assert model["fields"][1]["type"] == {"kind": "scalar", "name": "decimal128(18, 4)"}
    element = model["fields"][2]["type"]["fields"][0]["type"]["value"]
    assert element["nullable"] is False
    assert element["type"] == {"kind": "scalar", "name": "int32"}


def test_metadata_mode_is_explicit_and_affects_fingerprint() -> None:
    left = pa.schema(
        [pa.field("id", pa.int64(), metadata={b"unit": b"one"})], metadata={b"x": b"1"}
    )
    right = pa.schema(
        [pa.field("id", pa.int64(), metadata={b"unit": b"two"})], metadata={b"x": b"2"}
    )

    assert fingerprint_schema(schema_to_model(left, metadata="ignore")) == fingerprint_schema(
        schema_to_model(right, metadata="ignore")
    )
    assert fingerprint_schema(schema_to_model(left, metadata="exact")) != fingerprint_schema(
        schema_to_model(right, metadata="exact")
    )
    encoded = schema_to_model(left, metadata="exact")
    assert encoded["metadata"] == [{"key": "eA==", "value": "MQ=="}]
    assert encoded["fields"][0]["metadata"] == [{"key": "dW5pdA==", "value": "b25l"}]


def test_fingerprint_is_stable_for_equivalent_schemas() -> None:
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())])

    first = fingerprint_schema(schema_to_model(schema, metadata="ignore"))
    second = fingerprint_schema(schema_to_model(schema, metadata="ignore"))

    assert first == second
    assert first.startswith("sha256:")
