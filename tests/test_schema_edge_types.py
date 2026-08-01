from __future__ import annotations

import pyarrow as pa
import pytest

from parquet_drift.schema import diff_schemas, schema_to_model


def field_model(data_type: pa.DataType):
    schema = schema_to_model(pa.schema([pa.field("value", data_type)]), metadata="ignore")
    return schema["fields"][0]["type"]


def test_container_types_have_structural_models() -> None:
    fixed = field_model(pa.list_(pa.int16(), list_size=3))
    large = field_model(pa.large_list(pa.string()))
    mapping = field_model(pa.map_(pa.string(), pa.int64(), keys_sorted=True))
    dictionary = field_model(pa.dictionary(pa.int16(), pa.string(), ordered=True))

    assert fixed["kind"] == "fixed_size_list"
    assert fixed["size"] == 3
    assert large["kind"] == "large_list"
    assert mapping["kind"] == "map"
    assert mapping["keys_sorted"] is True
    assert dictionary == {
        "index": {"kind": "scalar", "name": "int16"},
        "kind": "dictionary",
        "ordered": True,
        "value": {"kind": "scalar", "name": "string"},
    }


def test_container_parameter_changes_are_type_changes() -> None:
    reference = schema_to_model(
        pa.schema([("value", pa.list_(pa.int16(), list_size=3))]), metadata="ignore"
    )
    actual = schema_to_model(
        pa.schema([("value", pa.list_(pa.int16(), list_size=4))]), metadata="ignore"
    )

    fixed_difference = diff_schemas(reference, actual)[0]
    assert fixed_difference["code"] == "type_changed"
    assert fixed_difference["expected"] != fixed_difference["actual"]

    reference = schema_to_model(
        pa.schema([("value", pa.map_(pa.string(), pa.int64(), keys_sorted=False))]),
        metadata="ignore",
    )
    actual = schema_to_model(
        pa.schema([("value", pa.map_(pa.string(), pa.int64(), keys_sorted=True))]),
        metadata="ignore",
    )
    map_difference = diff_schemas(reference, actual)[0]
    assert map_difference["code"] == "type_changed"
    assert map_difference["expected"] != map_difference["actual"]

    reference = schema_to_model(
        pa.schema([("value", pa.dictionary(pa.int16(), pa.string(), ordered=False))]),
        metadata="ignore",
    )
    actual = schema_to_model(
        pa.schema([("value", pa.dictionary(pa.int16(), pa.string(), ordered=True))]),
        metadata="ignore",
    )
    dictionary_difference = diff_schemas(reference, actual)[0]
    assert dictionary_difference["code"] == "type_changed"
    assert dictionary_difference["expected"] != dictionary_difference["actual"]


def test_dictionary_differences_identify_index_and_value_roles() -> None:
    reference = schema_to_model(
        pa.schema([("value", pa.dictionary(pa.int16(), pa.string()))]), metadata="ignore"
    )
    actual = schema_to_model(
        pa.schema([("value", pa.dictionary(pa.int32(), pa.binary()))]), metadata="ignore"
    )

    differences = diff_schemas(reference, actual)

    assert [item["path"] for item in differences] == ["/value/{index}", "/value/{value}"]
    assert all(item["code"] == "type_changed" for item in differences)


def test_union_types_preserve_mode_codes_and_fields() -> None:
    sparse = pa.sparse_union([pa.field("text", pa.string()), pa.field("number", pa.int64())])
    dense = pa.dense_union([pa.field("text", pa.string()), pa.field("number", pa.int64())])
    sparse_model = schema_to_model(pa.schema([("value", sparse)]), metadata="ignore")
    dense_model = schema_to_model(pa.schema([("value", dense)]), metadata="ignore")

    encoded = sparse_model["fields"][0]["type"]
    assert encoded["kind"] == "union"
    assert encoded["mode"] == "sparse"
    assert diff_schemas(sparse_model, dense_model)[0]["code"] == "type_changed"

    alternate_codes = pa.sparse_union(
        [pa.field("text", pa.string()), pa.field("number", pa.int64())], type_codes=[4, 7]
    )
    alternate_model = schema_to_model(pa.schema([("value", alternate_codes)]), metadata="ignore")
    code_difference = diff_schemas(sparse_model, alternate_model)[0]
    assert code_difference["expected"] != code_difference["actual"]


class _UuidType(pa.ExtensionType):
    def __init__(self) -> None:
        super().__init__(pa.binary(16), "tests.uuid")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def test_extension_type_name_and_storage_are_preserved() -> None:
    encoded = field_model(_UuidType())

    assert encoded == {
        "kind": "extension",
        "name": "tests.uuid",
        "serialized": "",
        "storage": {"kind": "scalar", "name": "fixed_size_binary[16]"},
    }


class _StateType(pa.ExtensionType):
    def __init__(self, state: bytes) -> None:
        self.state = state
        super().__init__(pa.int64(), "tests.state")

    def __arrow_ext_serialize__(self) -> bytes:
        return self.state

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls(serialized)


def test_extension_serialized_state_has_an_explainable_diff() -> None:
    reference = schema_to_model(pa.schema([("value", _StateType(b"one"))]), metadata="ignore")
    actual = schema_to_model(pa.schema([("value", _StateType(b"two"))]), metadata="ignore")

    difference = diff_schemas(reference, actual)[0]

    assert difference["code"] == "type_changed"
    assert difference["expected"] != difference["actual"]


def test_invalid_metadata_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="metadata must be"):
        schema_to_model(pa.schema([]), metadata="sometimes")  # type: ignore[arg-type]
