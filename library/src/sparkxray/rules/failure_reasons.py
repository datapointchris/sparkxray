"""SQL that reads a failure Spark reported: the one line naming the error, whether it was a cancellation, and the stage it blames."""

from __future__ import annotations

_TRACEBACK = 'Traceback (most recent call last):'

# DAGScheduler ends a job it cancels, and each running stage only that job ran, with `Job N cancelled <reason>`.
# Spark 4 raises it as the SPARK_JOB_CANCELLED condition, whose message leads with the condition's name. A
# SparkContext stopping ends its running stages with the second wording.
_CANCELLATION = r'^(\[SPARK_JOB_CANCELLED\] )?Job [0-9]+ cancelled|^Stage cancelled because SparkContext was shut down'
# A task's failure names its stage, as `Task 0 in stage 2.0 failed 4 times`, and a stage's own names it as
# `ShuffleMapStage 3 (...) has failed the maximum allowable number of times`.
_BLAMED_STAGE = r'(?:in stage |ShuffleMapStage |ResultStage )([0-9]+)'


def is_cancellation(column: str) -> str:
    """SQL that is true where the reason in `column` is Spark cancelling a job or its stages, not a failure."""
    return f"coalesce(regexp_matches({column}, '{_CANCELLATION}'), false)"


def blamed_stage(column: str) -> str:
    """SQL for the id of the first stage the reason in `column` names, or NULL where it names none."""
    return f"try_cast(nullif(regexp_extract({column}, '{_BLAMED_STAGE}', 1), '') AS BIGINT)"


# A Python error reaches the JVM as its traceback, whose first line ends in the header above. The
# error is named on the first unindented line after the last frame. Python indents a frame by two
# spaces, and the JVM indents its own stack frames by a tab, so the error is found before them.
_PYTHON_ERROR = r'(?s).*\n  [^\n]*\n([^ \t\n][^\n]*)'


def failure_line(column: str) -> str:
    """SQL for the first line of the text in `column`, a Python traceback's header replaced by the error it ends with."""
    first = f'split_part({column}, chr(10), 1)'
    error = f"coalesce(nullif(regexp_extract({column}, '{_PYTHON_ERROR}', 1), ''), '{_TRACEBACK}')"
    return f"CASE WHEN {first} LIKE '%{_TRACEBACK}' THEN replace({first}, '{_TRACEBACK}', {error}) ELSE {first} END"
