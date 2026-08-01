"""Canonical Arrow-visible schemas and structural diffs."""

from __future__ import annotations

import base64
import hashlib
import json
from collections import Counter
from typing import Any, Literal

import pyarrow as pa

MetadataMode = Literal["ignore", "exact"]
SchemaModel = dict[str, Any]
Difference = dict[str, Any]


def schema_to_model(schema: pa.Schema, *, metadata: MetadataMode) -> SchemaModel:
    """Convert an Arrow schema into a deterministic, JSON-compatible model."""
    if metadata not in ("ignore", "exact"):
        raise ValueError("metadata must be 'ignore' or 'exact'")

    model: SchemaModel = {"fields": [_field_to_model(field, metadata) for field in schema]}
    if metadata == "exact":
        model["metadata"] = _metadata_to_model(schema.metadata)
    return model


def fingerprint_schema(model: SchemaModel) -> str:
    """Return the stable SHA-256 fingerprint for a canonical schema model."""
    payload = json.dumps(model, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return f"sha256:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def diff_schemas(reference: SchemaModel, actual: SchemaModel) -> list[Difference]:
    """Return deterministic, field-level differences from reference to actual."""
    differences: list[Difference] = []
    if reference.get("metadata") != actual.get("metadata"):
        differences.append(
            {
                "actual": "changed",
                "code": "schema_metadata_changed",
                "expected": "exact match",
                "path": "/",
            }
        )
    _compare_fields(reference["fields"], actual["fields"], "/", differences)
    return differences


def _metadata_to_model(metadata: dict[bytes, bytes] | None) -> list[dict[str, str]]:
    if not metadata:
        return []
    return [
        {
            "key": base64.b64encode(key).decode("ascii"),
            "value": base64.b64encode(value).decode("ascii"),
        }
        for key, value in sorted(metadata.items())
    ]


def _field_to_model(field: pa.Field, metadata: MetadataMode) -> dict[str, Any]:
    model: dict[str, Any] = {
        "name": field.name,
        "nullable": field.nullable,
        "type": _type_to_model(field.type, metadata),
    }
    if metadata == "exact":
        model["metadata"] = _metadata_to_model(field.metadata)
    return model


def _type_to_model(data_type: pa.DataType, metadata: MetadataMode) -> dict[str, Any]:
    if pa.types.is_struct(data_type):
        return {
            "kind": "struct",
            "fields": [_field_to_model(field, metadata) for field in data_type],
        }
    if pa.types.is_fixed_size_list(data_type):
        return {
            "kind": "fixed_size_list",
            "size": data_type.list_size,
            "value": _field_to_model(data_type.value_field, metadata),
        }
    if pa.types.is_large_list(data_type):
        return {
            "kind": "large_list",
            "value": _field_to_model(data_type.value_field, metadata),
        }
    if pa.types.is_list(data_type):
        return {
            "kind": "list",
            "value": _field_to_model(data_type.value_field, metadata),
        }
    if pa.types.is_map(data_type):
        return {
            "kind": "map",
            "item": _field_to_model(data_type.item_field, metadata),
            "key": _field_to_model(data_type.key_field, metadata),
            "keys_sorted": data_type.keys_sorted,
        }
    if pa.types.is_dictionary(data_type):
        return {
            "kind": "dictionary",
            "index": _type_to_model(data_type.index_type, metadata),
            "ordered": data_type.ordered,
            "value": _type_to_model(data_type.value_type, metadata),
        }
    if pa.types.is_union(data_type):
        return {
            "kind": "union",
            "fields": [
                _field_to_model(data_type.field(index), metadata)
                for index in range(data_type.num_fields)
            ],
            "mode": data_type.mode,
            "type_codes": list(data_type.type_codes),
        }
    if isinstance(data_type, pa.ExtensionType):
        return {
            "kind": "extension",
            "name": data_type.extension_name,
            "storage": _type_to_model(data_type.storage_type, metadata),
        }
    return {"kind": "scalar", "name": str(data_type)}


def _compare_fields(
    reference: list[dict[str, Any]],
    actual: list[dict[str, Any]],
    path: str,
    differences: list[Difference],
) -> None:
    reference_names = [field["name"] for field in reference]
    actual_names = [field["name"] for field in actual]
    duplicate_names = sorted(
        {
            name
            for name, count in (Counter(reference_names) | Counter(actual_names)).items()
            if count > 1
        }
    )
    if duplicate_names:
        for name in duplicate_names:
            differences.append(
                {
                    "actual": actual_names.count(name),
                    "code": "duplicate_field_name",
                    "expected": reference_names.count(name),
                    "path": _join_path(path, name),
                }
            )
        return

    reference_by_name = {field["name"]: field for field in reference}
    actual_by_name = {field["name"]: field for field in actual}

    for name in reference_names:
        if name not in actual_by_name:
            differences.append(
                {
                    "actual": "missing",
                    "code": "field_removed",
                    "expected": _render_type(reference_by_name[name]["type"]),
                    "path": _join_path(path, name),
                }
            )
    for name in actual_names:
        if name not in reference_by_name:
            differences.append(
                {
                    "actual": _render_type(actual_by_name[name]["type"]),
                    "code": "field_added",
                    "expected": "absent",
                    "path": _join_path(path, name),
                }
            )

    if set(reference_names) == set(actual_names) and reference_names != actual_names:
        differences.append(
            {
                "actual": actual_names,
                "code": "field_order_changed",
                "expected": reference_names,
                "path": path,
            }
        )

    for name in reference_names:
        if name in actual_by_name:
            _compare_field(
                reference_by_name[name],
                actual_by_name[name],
                _join_path(path, name),
                differences,
            )


def _compare_field(
    reference: dict[str, Any],
    actual: dict[str, Any],
    path: str,
    differences: list[Difference],
    *,
    compare_name: bool = True,
) -> None:
    if compare_name and reference["name"] != actual["name"]:
        differences.append(
            {
                "actual": actual["name"],
                "code": "field_name_changed",
                "expected": reference["name"],
                "path": path,
            }
        )
    if reference["nullable"] != actual["nullable"]:
        differences.append(
            {
                "actual": "nullable" if actual["nullable"] else "required",
                "code": "nullability_changed",
                "expected": "nullable" if reference["nullable"] else "required",
                "path": path,
            }
        )
    if reference.get("metadata") != actual.get("metadata"):
        differences.append(
            {
                "actual": "changed",
                "code": "field_metadata_changed",
                "expected": "exact match",
                "path": path,
            }
        )
    _compare_type(reference["type"], actual["type"], path, differences)


def _compare_type(
    reference: dict[str, Any],
    actual: dict[str, Any],
    path: str,
    differences: list[Difference],
) -> None:
    if reference["kind"] != actual["kind"]:
        _append_type_change(reference, actual, path, differences)
        return

    kind = reference["kind"]
    if kind == "scalar":
        if reference["name"] != actual["name"]:
            _append_type_change(reference, actual, path, differences)
        return
    if kind == "struct":
        _compare_fields(reference["fields"], actual["fields"], path, differences)
        return
    if kind in {"list", "large_list", "fixed_size_list"}:
        if reference.get("size") != actual.get("size"):
            _append_type_change(reference, actual, path, differences)
            return
        _compare_field(
            reference["value"],
            actual["value"],
            _join_path(path, "[]"),
            differences,
            compare_name=False,
        )
        return
    if kind == "map":
        if reference["keys_sorted"] != actual["keys_sorted"]:
            _append_type_change(reference, actual, path, differences)
            return
        _compare_field(
            reference["key"],
            actual["key"],
            _join_path(path, "{key}"),
            differences,
            compare_name=False,
        )
        _compare_field(
            reference["item"],
            actual["item"],
            _join_path(path, "{value}"),
            differences,
            compare_name=False,
        )
        return
    if kind == "dictionary":
        if reference["ordered"] != actual["ordered"]:
            _append_type_change(reference, actual, path, differences)
            return
        _compare_type(reference["index"], actual["index"], _join_path(path, "{index}"), differences)
        _compare_type(reference["value"], actual["value"], _join_path(path, "{value}"), differences)
        return
    if kind == "union":
        if reference["mode"] != actual["mode"] or reference["type_codes"] != actual["type_codes"]:
            _append_type_change(reference, actual, path, differences)
            return
        _compare_fields(reference["fields"], actual["fields"], path, differences)
        return
    if kind == "extension":
        if reference["name"] != actual["name"]:
            _append_type_change(reference, actual, path, differences)
            return
        _compare_type(reference["storage"], actual["storage"], path, differences)
        return
    if reference != actual:
        _append_type_change(reference, actual, path, differences)


def _append_type_change(
    reference: dict[str, Any],
    actual: dict[str, Any],
    path: str,
    differences: list[Difference],
) -> None:
    differences.append(
        {
            "actual": _render_type(actual),
            "code": "type_changed",
            "expected": _render_type(reference),
            "path": path,
        }
    )


def _render_type(model: dict[str, Any]) -> str:
    kind = model["kind"]
    if kind == "scalar":
        return model["name"]
    if kind == "struct":
        fields = ", ".join(
            f"{field['name']}: {_render_type(field['type'])}" for field in model["fields"]
        )
        return f"struct<{fields}>"
    if kind in {"list", "large_list", "fixed_size_list"}:
        size = f"[{model['size']}]" if kind == "fixed_size_list" else ""
        return f"{kind}{size}<{_render_type(model['value']['type'])}>"
    if kind == "map":
        return f"map<{_render_type(model['key']['type'])}, {_render_type(model['item']['type'])}>"
    if kind == "dictionary":
        return f"dictionary<{_render_type(model['index'])}, {_render_type(model['value'])}>"
    if kind == "extension":
        return f"extension<{model['name']}>"
    if kind == "union":
        return f"{model['mode']}_union"
    return json.dumps(model, ensure_ascii=False, sort_keys=True)


def _join_path(path: str, segment: str) -> str:
    escaped = segment.replace("~", "~0").replace("/", "~1")
    return f"/{escaped}" if path == "/" else f"{path}/{escaped}"
