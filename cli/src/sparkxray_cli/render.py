"""How the CLI prints: data to stdout as a table or as JSON, and progress, notes and errors to stderr.

A table shows each value in the unit its column's name ends in, as the Spark UI shows it: bytes as
`702.5 MiB`, a duration as `1.9 s`, an instant as local time. JSON keeps the raw numbers.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import sys
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Literal

from rich import box
from rich.console import Console
from rich.console import JustifyMethod
from rich.table import Table
from rich.text import Text
from sparkxray import spark_ui_format
from sparkxray.errors import SparkxrayError
from sparkxray.ingest import IngestManifest
from sparkxray.rules import Severity
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


def emit_json(data: Any) -> None:
    """Print data as JSON with a bare print, so no markup or color reaches a pipe."""
    print(json.dumps(data, indent=2))


def emit_text(text: str) -> None:
    """Write rendered text to stdout as it is, so no markup or color reaches a pipe."""
    sys.stdout.write(text)


def shown(name: str, value: Any) -> str:
    """A value as a person reads it, in the unit its name ends in."""
    return shown_in(column_unit(name), value)


def shown_in(unit: str | None, value: Any) -> str:
    """A value as a person reads it, in a unit `column_unit` or a threshold names, an instant in local time."""
    return spark_ui_format.shown_in(unit, value, local_time=True)


def table(*, title: str | None = None, show_header: bool = True) -> Table:
    """A table with no vertical rules and two spaces between columns, which leaves the width to the values."""
    return Table(
        title=title,
        title_justify='left',
        title_style='bold',
        show_header=show_header,
        box=box.SIMPLE_HEAD,
        show_edge=False,
        padding=(0, 1, 0, 0),
        pad_edge=False,
    )


def header(column: str) -> str:
    """A column's name with spaces for underscores, so a narrow column wraps it between words."""
    return column.replace('_', ' ')


def is_handle(column: str) -> bool:
    """An id a person types back into another command, so its cell is never wrapped."""
    return column.endswith('_id')


def print_rows(rows: Rows, columns: Sequence[str] | None = None, *, title: str | None = None) -> None:
    """Print rows as a table of the named columns, or of all of them."""
    print_records(rows.to_json(), [column for column in (columns or rows.columns) if column in rows.columns], title=title)


def print_records(
    records: Sequence[Mapping[str, Any]], columns: Sequence[str], *, title: str | None = None, numbers: Sequence[str] = ()
) -> None:
    """Print mappings as a table of the named columns, numbers right-aligned, every row one line.

    A table wider than the terminal is laid out again. A number, a handle or a short one-word
    value then keeps its width, and only its header wraps, between words. A column of longer text
    gives up the rest of the width, and each of its cells ends in an ellipsis rather than
    wrapping. Where that would leave a text column under `_TEXT_FLOOR` characters, each record
    prints as its own block of field and value instead, as psql's expanded display does.

    `numbers` names columns whose values are numbers already shown as text, as when each row's
    unit differs. They print as numbers do.

    Every cell is a `Text`, so a bracket in a value, as in a plan's `Project [id#0L]`, prints
    as written rather than being read as a style.
    """
    cells = {column: [_cell(column, record.get(column)) for record in records] for column in columns}
    numeric = {column: column in numbers or any(_is_number(record.get(column)) for record in records) for column in columns}
    natural = _records_table(columns, cells, numeric, title, 'natural')
    if not records or _natural_width(natural) <= stdout.width:
        print_table(natural)
    elif _natural_width(_records_table(columns, cells, numeric, title, 'floored')) <= stdout.width:
        print_table(_records_table(columns, cells, numeric, title, 'fitted'))
    else:
        if title:
            stdout.print(Text(title, style='bold'))
        for record in records:
            print_record(record, columns)


# A one-word value up to this long keeps its width in a fitted table, as a status or a host does.
_KEPT_WORD_LENGTH = 20
# The fewest characters of a text column a fitted table shows, enough for a stage name's action.
_TEXT_FLOOR = 12

_Layout = Literal['natural', 'floored', 'fitted']


def _cell(column: str, value: Any) -> Text:
    """One line of a value as a person reads it."""
    return Text(shown(column, value), style=_style(column, value), no_wrap=True, overflow='ellipsis')


def _style(column: str, value: Any) -> str:
    """A severity prints in its level's color, and every other value plain."""
    return SEVERITY_STYLES.get(value, '') if column == 'severity' else ''


def _records_table(
    columns: Sequence[str], cells: Mapping[str, Sequence[Text]], numeric: Mapping[str, bool], title: str | None, layout: _Layout
) -> Table:
    printed = table(title=title)
    for column in columns:
        justify: JustifyMethod = 'right' if numeric[column] else 'left'
        values = cells[column]
        if layout == 'natural':
            printed.add_column(header(column), justify=justify, no_wrap=True)
            continue
        heading = Text(header(column), no_wrap=False, overflow='fold')
        if numeric[column] or is_handle(column) or all(cell.cell_len <= _KEPT_WORD_LENGTH and ' ' not in cell.plain for cell in values):
            width = max([len(word) for word in header(column).split()] + [cell.cell_len for cell in values])
            printed.add_column(heading, justify=justify, no_wrap=True, max_width=width)
        else:
            printed.add_column(heading, justify=justify, max_width=_TEXT_FLOOR if layout == 'floored' else None)
    for row in zip(*(cells[column] for column in columns), strict=True):
        printed.add_row(*row)
    return printed


def _natural_width(printed: Table) -> int:
    """How wide a table prints with no terminal to fit, since a measure at the terminal's width stops at it."""
    return stdout.measure(printed, options=stdout.options.update_width(sys.maxsize)).maximum


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def print_record(record: Mapping[str, Any], keys: Sequence[str] | None = None, *, title: str | None = None) -> None:
    """Print one record as a table of field and value, each field under the name --json gives it."""
    printed = table(title=title, show_header=False)
    printed.add_column('field', style='bold')
    printed.add_column('value', overflow='fold')
    for key in keys or list(record):
        if key in record:
            printed.add_row(Text(key), Text(shown(key, record[key]), style=_style(key, record[key])))
    print_table(printed)


def print_described(
    title: str, records: Sequence[Mapping[str, Any]], columns: Sequence[str], *, summary: str = '', location: str | None = None
) -> None:
    """Print a titled table whose last column is prose that wraps, every other column kept whole on its line.

    `summary` and then `location` print between the title and the table. A location is never
    wrapped, so a path pastes whole.
    """
    stdout.print(Text(title, style='bold'))
    if summary:
        stdout.print(Text(summary))
    if location:
        stdout.print(Text(location, style='dim'), soft_wrap=True)
    printed = table()
    for column in columns[:-1]:
        printed.add_column(header(column), no_wrap=True)
    printed.add_column(header(columns[-1]))
    for record in records:
        printed.add_row(*(Text(shown(column, record.get(column))) for column in columns))
    print_table(printed)


def print_metric_changes(records: Sequence[Mapping[str, Any]], *, title: str) -> None:
    """Print the metrics that moved, a table for each part of the run under a line naming the part and how it was paired.

    Each record carries `part`, `matched_by`, and a metric's fields as `MetricDelta.to_json` gives
    them, the records of one part together. Each value prints in the record's own unit, and the
    change as a share of run A's. A part's line is never wrapped, so its pairing reads whole.
    """
    stdout.print(Text(title, style='bold'))
    for (part, matched_by), metrics in itertools.groupby(records, key=lambda record: (record['part'], record['matched_by'])):
        stdout.print(Text(f'{part}, paired by {matched_by}'), soft_wrap=True)
        rows = [
            {
                'metric': record['metric'],
                'a': shown_in(record['unit'], record['a']),
                'b': shown_in(record['unit'], record['b']),
                'change': '' if record['delta_share'] is None else f'{record["delta_share"] * 100:+.0f}%',
            }
            for record in metrics
        ]
        print_records(rows, ('metric', 'a', 'b', 'change'), numbers=('a', 'b', 'change'))


def print_table(printed: Table) -> None:
    """Print a table and a blank line after it, which sets it apart from the next section."""
    stdout.print(printed)
    stdout.print()


def print_list(title: str, items: Sequence[str]) -> None:
    """Print a titled bullet list and a blank line after it, nothing when it is empty."""
    if items:
        stdout.print(Text(title, style='bold'))
        for item in items:
            stdout.print(Text(f'- {item}'))
        stdout.print()


def print_links(links: Sequence[str]) -> None:
    """Print each URL on its own line, never wrapped, so a terminal that links URLs links them whole."""
    if links:
        stdout.print(Text('Read more', style='bold'))
        for link in links:
            stdout.print(Text(link, style=f'link {link}'), soft_wrap=True)
        stdout.print()


def note(message: str) -> None:
    """A line for the person at the terminal, never part of the data. It is never wrapped, so a command in it pastes whole."""
    stderr.print(Text(message, style='dim'), soft_wrap=True)


def none_found(what: str) -> None:
    """Say on stderr that a list came back empty, so an empty table is never left to be read as a failure."""
    note(f'no {what}')


def report_error(error: SparkxrayError, *, as_json: bool = False) -> None:
    """The library's refusal, with its code, the values that would have worked, and its hint: on one JSON line where `as_json`."""
    if as_json:
        sys.stderr.write(json.dumps({'error': error.to_json()}, ensure_ascii=False) + '\n')
        return
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
