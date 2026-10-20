"""The arguments and options more than one command takes, so each reads and helps the same way everywhere."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from sparkxray.rules import Severity

LOG_HELP = (
    'A log file or directory, an s3:// URI to one, the http(s):// address of a History Server or driver UI or of an '
    'application on one, or the app id of a log already read.'
)
LogArgument = Annotated[str, typer.Argument(metavar='LOG', help=LOG_HELP, show_default=False)]
JsonOption = Annotated[bool, typer.Option('--json', help='Print JSON to stdout instead of a table, and a refusal as JSON on stderr.')]
LimitOption = Annotated[
    int | None,
    typer.Option('--limit', '-n', min=0, help='Keep only the first N rows. Every row by default.', show_default=False),
]
SortOption = Annotated[
    str | None,
    typer.Option(
        '--sort',
        metavar='COLUMN',
        help=(
            'Sort by this column, largest first: any name --json prints, which is the header with underscores for spaces, '
            'as wall_ms for the wall ms column. Its unit is the one the name ends in, whichever the table prints.'
        ),
        show_default=False,
    ),
]
ThresholdOption = Annotated[
    list[str] | None,
    typer.Option(
        '--threshold',
        metavar='RULE_ID.NAME=VALUE',
        help=(
            'Override one threshold, as stage.shuffle-skew.ratio=8 or stage.shuffle-skew.min_bytes=512MiB. '
            "sparkxray rules show RULE_ID names a rule's thresholds. Repeat for more."
        ),
        show_default=False,
    ),
]
FailOnOption = Annotated[
    Severity, typer.Option('--fail-on', help='Exit 3 when a finding is at or above this severity, whether or not it is listed.')
]
ThresholdFileOption = Annotated[
    Path | None,
    typer.Option(
        '--thresholds',
        metavar='FILE',
        help='A TOML file of thresholds, one table per rule, as ["stage.shuffle-skew"] then ratio = 8. A --threshold beats it.',
        show_default=False,
    ),
]
