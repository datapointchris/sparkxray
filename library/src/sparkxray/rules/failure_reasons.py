"""SQL that cuts a failure Spark reported to the one line naming the error."""

from __future__ import annotations

_TRACEBACK = 'Traceback (most recent call last):'

# A Python error reaches the JVM as its traceback, whose first line ends in the header above. The
# error is named on the first unindented line after the last frame. Python indents a frame by two
# spaces, and the JVM indents its own stack frames by a tab, so the error is found before them.
_PYTHON_ERROR = r'(?s).*\n  [^\n]*\n([^ \t\n][^\n]*)'


def failure_line(column: str) -> str:
    """SQL for the first line of the text in `column`, a Python traceback's header replaced by the error it ends with."""
    first = f'split_part({column}, chr(10), 1)'
    error = f"coalesce(nullif(regexp_extract({column}, '{_PYTHON_ERROR}', 1), ''), '{_TRACEBACK}')"
    return f"CASE WHEN {first} LIKE '%{_TRACEBACK}' THEN replace({first}, '{_TRACEBACK}', {error}) ELSE {first} END"
