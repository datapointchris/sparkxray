"""How the CLI prints: data to stdout as a table or as JSON, and progress, notes and errors to stderr.

A table shows each value in the unit its column's name ends in, as the Spark UI shows it: bytes as
`702.5 MiB`, a duration as `1.9 s`, an instant as local time. JSON keeps the raw numbers.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text
from sparkxray.errors import SparkxrayError
from sparkxray.ingest import IngestManifest
from sparkxray.rules import Severity
from sparkxray.spark_ui_format import bytes_to_string
from sparkxray.spark_ui_format import format_duration
from sparkxray.tables.registry import column_unit
from sparkxray.tables.rows import Rows

stdout = Console(highlight=False)
stderr = Console(stderr=True, highlight=False)

SEVERITY_STYLES: Mapping[Severity, str] = {
    'info': 'dim',
    'low': 'cyan',
    'medium': 'yellow',
    'high': 'red',
    'critical': 'bold red',
}
_GROUPED_UNITS = frozenset({'count', 'records', 'rows'})


def emit_json(data: Any) -> None:
    """Print data as JSON with a bare print, so no markup or color reaches a pipe."""
    print(json.dumps(data, indent=2))


def shown(name: str, value: Any) -> str:
    """A value as a person reads it, in the unit its name ends in."""
    return shown_in(column_unit(name), value)


def shown_in(unit: str | None, value: Any) -> str:
    """A value as a person reads it, in a unit `column_unit` or a threshold names."""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    if isinstance(value, list):
        return ', '.join(shown_in(unit, item) for item in value)
    if isinstance(value, dict):
        return ', '.join(f'{key}={item}' for key, item in value.items())
    if not isinstance(value, int | float):
        return str(value)
    if unit == 'bytes':
        return bytes_to_string(int(value))
    if unit == 'ms':
        return format_duration(int(value))
    if unit == 'ns':
        return format_duration(int(value) // 1_000_000)
    if unit == 's':
        return format_duration(round(value * 1000))
    if unit == 'epoch_ms':
        return dt.datetime.fromtimestamp(value / 1000).strftime('%Y-%m-%d %H:%M:%S')
    if unit == 'fraction':
        return f'{value * 100:.1f}%'
    if unit == 'x':
        return f'{value:g}x'
    if unit in _GROUPED_UNITS:
        return f'{value:,}'
    return str(value)


def table(*, title: str | None = None, show_header: bool = True) -> Table:
    """A table with no vertical rules, which leaves the width to the values."""
    return Table(title=title, title_justify='left', title_style='bold', show_header=show_header, box=box.SIMPLE_HEAD, pad_edge=False)


def header(column: str) -> str:
    """A column's name with spaces for underscores, so a narrow column wraps it between words."""
    return column.replace('_', ' ')


def is_handle(column: str) -> bool:
    """An id a person types back into another command, so its cell is never wrapped."""
    return column.endswith('_id')


def print_rows(rows: Rows, columns: Sequence[str] | None = None, *, title: str | None = None) -> None:
    """Print rows as a table of the named columns, or of all of them."""
    print_records(rows.to_json(), [column for column in (columns or rows.columns) if column in rows.columns], title=title)


def print_records(records: Sequence[Mapping[str, Any]], columns: Sequence[str], *, title: str | None = None) -> None:
    """Print mappings as a table of the named columns, numbers right-aligned."""
    printed = table(title=title)
    for column in columns:
        numeric = any(isinstance(record.get(column), int | float) and not isinstance(record.get(column), bool) for record in records)
        printed.add_column(header(column), justify='right' if numeric else 'left', overflow='fold', no_wrap=is_handle(column))
    for record in records:
        printed.add_row(*(Text(shown(column, record.get(column))) for column in columns))
    stdout.print(printed)


def print_record(record: Mapping[str, Any], keys: Sequence[str] | None = None, *, title: str | None = None) -> None:
    """Print one record as a table of field and value, each field under the name --json gives it."""
    printed = table(title=title, show_header=False)
    printed.add_column('field', style='bold')
    printed.add_column('value', overflow='fold')
    for key in keys or list(record):
        if key in record:
            printed.add_row(key, Text(shown(key, record[key])))
    stdout.print(printed)


def print_list(title: str, items: Sequence[str]) -> None:
    """Print a titled bullet list, nothing when it is empty."""
    if items:
        stdout.print(Text(title, style='bold'))
        for item in items:
            stdout.print(Text(f'- {item}'))


def print_links(links: Sequence[str]) -> None:
    """Print each URL on its own line, never wrapped, so a terminal that links URLs links them whole."""
    if links:
        stdout.print(Text('Read more', style='bold'))
    for link in links:
        stdout.print(Text(link, style=f'link {link}'), soft_wrap=True)


def severity_text(severity: Severity) -> Text:
    return Text(severity, style=SEVERITY_STYLES[severity])


def note(message: str) -> None:
    """A line for the person at the terminal, never part of the data. It is never wrapped, so a command in it pastes whole."""
    stderr.print(Text(message, style='dim'), soft_wrap=True)


def none_found(what: str) -> None:
    """Say on stderr that a list came back empty, so an empty table is never left to be read as a failure."""
    note(f'no {what}')


def report_error(error: SparkxrayError) -> None:
    """The library's refusal, with its code, the values that would have worked, and its hint."""
    text = Text(str(error))
    text.stylize('bold red', 0, len(error.code))
    stderr.print(text, soft_wrap=True)


@contextlib.contextmanager
def status(message: str) -> Iterator[None]:
    """A spinner on stderr while something slow runs. It draws only on a terminal, and leaves nothing behind."""
    with stderr.status(Text(message), spinner='dots'):
        yield


def ingest_notes(manifest: IngestManifest) -> None:
    """Notes on stderr about what a log could not hold."""
    if manifest.in_progress:
        note(f'{manifest.app_id}: Spark was still writing this log, so it holds the run only as far as it had got')
    if manifest.truncated_tail:
        note(f"{manifest.app_id}: the log's last line was cut off partway, and is left out")
    if manifest.compacted:
        note(f'{manifest.app_id}: a History Server compacted this log, which dropped the events of finished tasks')
