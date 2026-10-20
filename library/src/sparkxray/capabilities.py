"""Everything a program driving sparkxray can call or read, gathered from the registries that run it.

The command surface comes from the caller, because only the command can walk its own tree. The
rules come from `catalog()`, the tables from the table registry, the schemas from
`sparkxray.schemas` and the codes from the error registry. So nothing here is written by hand,
and a rule or error added without docs still appears. `schemas/capabilities.json` is its schema.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from sparkxray.cache_directory import installed_version
from sparkxray.errors import registered
from sparkxray.rules.catalog import catalog
from sparkxray.rules.rule import SIGNALS
from sparkxray.schemas import queryable_tables
from sparkxray.schemas import schema_names

# A field removed or a meaning changed raises this. A field added does not.
SCHEMA_VERSION = 1


def capabilities(commands: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The document `capabilities show --json` prints, around the `commands` the caller walked from its own command tree."""
    return {
        'schema_version': SCHEMA_VERSION,
        'tool': 'sparkxray',
        'version': installed_version(),
        'commands': [dict(command) for command in commands],
        'rules': [
            {
                'rule_id': rule.rule_id,
                'rule_version': rule.rule_version,
                'scope': rule.scope,
                'signals': [signal for signal in SIGNALS if signal in rule.signals],
                'thresholds': [threshold.to_json() for threshold in rule.thresholds],
            }
            for rule in catalog()
        ],
        'signals': list(SIGNALS),
        'tables': [table.name for table in queryable_tables()],
        'schemas': list(schema_names()),
        'error_codes': [error.code for error in registered()],
    }
