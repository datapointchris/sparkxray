"""Every number a shipped schema describes names its unit, so a program reading sparkxray's JSON never guesses one."""

from typing import Any

from sparkxray.rules.findings import UNIT_SUFFIXES
from sparkxray.schemas import schema_document
from sparkxray.schemas import schema_names

# An attempt, a version, an index or a line number identifies something rather than measuring it, so it has no unit.
_IDENTIFIERS = ('attempt', 'version', '_index', 'line_number')


def test_every_numeric_property_of_every_schema_ends_in_its_unit_or_sits_beside_one() -> None:
    unitless = {name: _unitless(schema_document(name), name) for name in schema_names()}

    assert unitless == {name: [] for name in schema_names()}


def _unitless(node: Any, path: str) -> list[str]:
    found = []
    if isinstance(node, dict):
        declared = node.get('properties')
        properties: dict[str, Any] = declared if isinstance(declared, dict) else {}
        for key, field in properties.items():
            if _numeric(field) and not key.endswith(UNIT_SUFFIXES + _IDENTIFIERS) and 'unit' not in properties:
                found.append(f'{path}.{key}')
        for key, child in node.items():
            found += _unitless(child, f'{path}/{key}')
    elif isinstance(node, list):
        for index, child in enumerate(node):
            found += _unitless(child, f'{path}[{index}]')
    return found


def _numeric(field: Any) -> bool:
    if not isinstance(field, dict):
        return False
    kinds = field.get('type')
    constant = field.get('const')
    return bool({'integer', 'number'} & set(kinds if isinstance(kinds, list) else [kinds])) or (
        isinstance(constant, int | float) and not isinstance(constant, bool)
    )
