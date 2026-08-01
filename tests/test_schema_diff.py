from __future__ import annotations

import pyarrow as pa

from parquet_drift.schema import diff_schemas, schema_to_model


def model(schema: pa.Schema):
    return schema_to_model(schema, metadata="ignore")


def codes(reference: pa.Schema, actual: pa.Schema) -> list[str]:
    return [item["code"] for item in diff_schemas(model(reference), model(actual))]


def test_diff_reports_top_level_drift() -> None:
    reference = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())]
    )
    actual = pa.schema([pa.field("id", pa.int32()), pa.field("extra", pa.bool_())])

    differences = diff_schemas(model(reference), model(actual))

    assert [item["code"] for item in differences] == [
        "field_removed",
        "field_added",
        "nullability_changed",
        "type_changed",
    ]
    assert differences[0]["path"] == "/name"
    assert differences[1]["path"] == "/extra"
    assert differences[-1]["expected"] == "int64"
    assert differences[-1]["actual"] == "int32"


def test_diff_reports_field_order_without_duplicate_noise() -> None:
    reference = pa.schema([("a", pa.int64()), ("b", pa.int64())])
    actual = pa.schema([("b", pa.int64()), ("a", pa.int64())])

    differences = diff_schemas(model(reference), model(actual))

    assert differences == [
        {
            "actual": ["b", "a"],
            "code": "field_order_changed",
            "expected": ["a", "b"],
            "path": "/",
        }
    ]


def test_diff_recurses_through_struct_and_list_roles() -> None:
    reference = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct([pa.field("values", pa.list_(pa.field("element", pa.int32(), False)))]),
            )
        ]
    )
    actual = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct([pa.field("values", pa.list_(pa.field("item", pa.int64(), True)))]),
            )
        ]
    )

    differences = diff_schemas(model(reference), model(actual))

    assert differences == [
        {
            "actual": "nullable",
            "code": "nullability_changed",
            "expected": "required",
            "path": "/payload/values/[]",
        },
        {
            "actual": "int64",
            "code": "type_changed",
            "expected": "int32",
            "path": "/payload/values/[]",
        },
    ]


def test_paths_escape_real_slashes_and_tildes_in_field_names() -> None:
    reference = pa.schema([("a/b~c", pa.string())])
    actual = pa.schema([])

    assert diff_schemas(model(reference), model(actual))[0]["path"] == "/a~1b~0c"


def test_duplicate_names_fail_closed() -> None:
    reference = pa.schema([("id", pa.int64()), ("id", pa.int64())])
    actual = pa.schema([("id", pa.int64())])

    differences = diff_schemas(model(reference), model(actual))

    assert differences[0]["code"] == "duplicate_field_name"


def test_schema_metadata_changes_are_reported_in_exact_mode() -> None:
    reference = schema_to_model(
        pa.schema([("id", pa.int64())], metadata={b"x": b"1"}), metadata="exact"
    )
    actual = schema_to_model(
        pa.schema([("id", pa.int64())], metadata={b"x": b"2"}), metadata="exact"
    )

    assert diff_schemas(reference, actual)[0]["code"] == "schema_metadata_changed"
