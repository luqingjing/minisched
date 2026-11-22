"""
A deliberately small JSON Schema validator (stdlib only).

Supports the subset that structured-output prompts actually use:
  type (string | number | integer | boolean | object | array | null, or a list of them)
  properties, required, additionalProperties (bool)
  items, minItems, maxItems
  enum, const
  minimum, maximum, minLength, maxLength
Anything else is ignored, so an unsupported keyword never makes a valid
document fail. Returns a list of human-readable errors; empty list means valid.
"""
from __future__ import annotations

from typing import Any

_TYPES = {
    "string": lambda v: isinstance(v, str),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "null": lambda v: v is None,
}


def validate(value: Any, schema: dict, path: str = "$") -> list[str]:
    errs: list[str] = []
    if not isinstance(schema, dict):
        return errs

    t = schema.get("type")
    if t is not None:
        types = t if isinstance(t, list) else [t]
        if not any(_TYPES.get(x, lambda v: True)(value) for x in types):
            return [f"{path}: expected {'/'.join(types)}, got {type(value).__name__}"]

    if "const" in schema and value != schema["const"]:
        errs.append(f"{path}: must equal {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errs.append(f"{path}: {value!r} not in {schema['enum']!r}")

    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errs.append(f"{path}: shorter than {schema['minLength']}")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errs.append(f"{path}: longer than {schema['maxLength']}")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errs.append(f"{path}: {value} < minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errs.append(f"{path}: {value} > maximum {schema['maximum']}")

    if isinstance(value, dict):
        props = schema.get("properties", {})
        for key in schema.get("required", []):
            if key not in value:
                errs.append(f"{path}: missing required '{key}'")
        for key, sub in props.items():
            if key in value:
                errs += validate(value[key], sub, f"{path}.{key}")
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in props:
                    errs.append(f"{path}: unexpected property '{key}'")

    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errs.append(f"{path}: fewer than {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errs.append(f"{path}: more than {schema['maxItems']} items")
        if "items" in schema:
            for i, item in enumerate(value):
                errs += validate(item, schema["items"], f"{path}[{i}]")
    return errs
