"""Operator names, id extraction and label normalization, shared by every plan parser.

Every rewrite is anchored to the syntax that carries an id. A rule matching any bare number would
turn `count(1)` into `count(N)` and change an expression, so numeric literals, partition counts and
range bounds survive normalization. Spark prints string literals unquoted, so a literal holding `#`
and digits, such as `PR#42`, reads as an expression id.
"""

from __future__ import annotations

import dataclasses as dc
import re
from collections.abc import Iterable
from collections.abc import Mapping

QUERY_STAGE_OPERATORS = frozenset({'ResultQueryStage', 'ShuffleQueryStage', 'BroadcastQueryStage', 'TableCacheQueryStage'})
SUBQUERY_OPERATORS = frozenset({'Subquery', 'SubqueryBroadcast', 'SubqueryAdaptiveBroadcast', 'ReusedSubquery'})

_JOIN_TYPES = frozenset({'Inner', 'Cross', 'LeftOuter', 'RightOuter', 'FullOuter', 'LeftSemi', 'LeftAnti', 'LeftSingle', 'ExistenceJoin'})
_FIRST_TOKEN = re.compile(r'[^\s(\[]+')
_SCAN_FORMAT = re.compile(r'\s*([^\s(\[]+)')
_TRUNCATION = re.compile(r'\.\.\. \d+ more (?:fields|characters)|Truncated plan of \d+ characters')

_SUBQUERY_REFERENCE = re.compile(r'(?<![\w])(?:subquery|dynamicpruning)#(\d+)')
_EXPRESSION_ID = re.compile(r'(?<=[\w)\]`])#(\d+)L?(?!\w)')
_TRAILING_PLAN_ID = re.compile(r'\[(?:plan_id=|id=#)(\d+)\]\s*$')
_QUERY_STAGE_NUMBER = re.compile(r'QueryStage (\d+)')
_LEADING_NUMBER = re.compile(r'\s*(\d+)')

_REWRITES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r'"jvmId"\s*:\s*"[^"]*"\s*,?\s*'), ''),
    (re.compile(r'Statistics\([^()]*\)'), 'Statistics(<stats>)'),
    (re.compile(r'\$\$Lambda(?:\$\d+)?(?:/0x[0-9a-f]+)?(?:@[0-9a-f]+)?'), '$$Lambda'),
    (re.compile(r'(\.[A-Z][\w$]*)@[0-9a-f]{1,8}\b'), r'\1@N'),
    (re.compile(r'\b(?:file|s3[an]?|hdfs|abfss?|gs|wasbs?|dbfs|viewfs):/[^\s,\])]*'), '<path>'),
    (re.compile(r'\[id=#\d+\]'), '[id=N]'),
    (re.compile(r'plan_id=\d+'), 'plan_id=N'),
    (re.compile(r'(?<![\w])(subquery)#\d+'), r'\1N'),
    (re.compile(r'(?<![\w])(Subquery):\d+'), r'\1N'),
    (_EXPRESSION_ID, '#N'),
    (re.compile(r'(?m)^([ :]*(?:[+:]- )?)\*\(\d+\) '), r'\1*(N) '),
    (re.compile(r'(?m)^\(\d+\) '), '(N) '),
    (re.compile(r'(?m) \(\d+\)$'), ' (N)'),
    (re.compile(r'\(\d+\)(?=, Statistics\()'), '(N)'),
    (re.compile(r'\b((?:Result|Shuffle|Broadcast|TableCache)QueryStage) \d+'), r'\1 N'),
    (re.compile(r'codegen id : \d+'), 'codegen id : N'),
    (re.compile(r'WholeStageCodegen \(\d+\)'), 'WholeStageCodegen (N)'),
    (re.compile(r'Hosting operator id = \d+'), 'Hosting operator id = N'),
    (re.compile(r'Reuses operator id: \d+'), 'Reuses operator id: N'),
)
_STAGE_BLOCK_HEADER = re.compile(r'^\(\S+\) (?:Result|Shuffle|Broadcast|TableCache)QueryStage\b')
_STAGE_BLOCK_ARGUMENTS = re.compile(r'^Arguments: \d+')


def normalize_label(text: str) -> str:
    """Rewrite every id in a node label, or in a whole plan text, to a placeholder.

    Expression ids become `#N`, exchange ids `plan_id=N` and `[id=N]`, codegen stages `*(N)`,
    formatted node numbers `(N)`, query stage numbers `ShuffleQueryStage N`, subquery ids
    `subqueryN`, URIs `<path>`, statistics `Statistics(<stats>)`, and a JVM's lambda class and
    an object's identity hash after its class name, `$$Lambda` and `NoopWrite$@N`. A `"jvmId"`
    key is dropped.
    """
    for pattern, replacement in _REWRITES:
        text = pattern.sub(replacement, text)
    if '\n' not in text:
        return text
    lines = text.split('\n')
    in_stage_block = False
    for index, line in enumerate(lines):
        if not line:
            in_stage_block = False
        elif _STAGE_BLOCK_HEADER.match(line):
            in_stage_block = True
        elif in_stage_block and _STAGE_BLOCK_ARGUMENTS.match(line):
            lines[index] = _STAGE_BLOCK_ARGUMENTS.sub('Arguments: N', line)
    return '\n'.join(lines)


def normalize_detail(operator: str, key: str, value: str) -> str:
    """Normalize one details value. A query stage's `Arguments` is its stage id, so it becomes N."""
    if operator in QUERY_STAGE_OPERATORS and key == 'Arguments':
        return _LEADING_NUMBER.sub('N', value, count=1)
    return normalize_label(value)


def canonical_operator(name: str) -> tuple[str, str | None]:
    """The operator for a node name or cleaned label, and the scan format when it reads files.

    The operator is the first token, split on whitespace, `(` or `[`. A file scan prints as
    `FileScan parquet` in simple text and `Scan parquet` elsewhere, so both become `Scan` with the
    format `parquet`. A command runs as `Execute <command>`, so the command is the operator.
    """
    name = name.strip()
    if name.startswith('Execute '):
        name = name[len('Execute ') :].lstrip()
    match = _FIRST_TOKEN.match(name)
    if match is None:
        return name, None
    operator = match.group()
    if operator in ('Scan', 'FileScan'):
        scan_format = _SCAN_FORMAT.match(name, match.end())
        return 'Scan', scan_format.group(1) if scan_format else None
    return operator, None


def join_type(label: str) -> str | None:
    """The join type a join's label lists among its arguments, such as `Inner` or `LeftAnti`."""
    for argument in _top_level_arguments(label):
        token = _FIRST_TOKEN.match(argument)
        if token and token.group() in _JOIN_TYPES:
            return argument.strip()
    return None


def is_truncated(text: str) -> bool:
    """Whether Spark cut this plan text: `... N more fields`, `... N more characters`, or `Truncated plan of`."""
    return _TRUNCATION.search(text) is not None


@dc.dataclass(frozen=True, slots=True)
class NodeIds:
    plan_id: int | None
    query_stage_id: int | None


def extract_ids(operator: str, label: str, details: Mapping[str, str]) -> NodeIds:
    """The exchange or subquery id a node prints after its own arguments, and a query stage's id.

    The id is read from the end of the label, or from the end of a formatted `Arguments` line, so
    a subquery embedded in a filter's condition never lends the filter its `[id=#N]`.
    """
    plan_id = None
    for text in (label, details.get('Arguments', '')):
        match = _TRAILING_PLAN_ID.search(text)
        if match:
            plan_id = int(match.group(1))
            break
    query_stage_id = None
    if operator in QUERY_STAGE_OPERATORS:
        stage = _QUERY_STAGE_NUMBER.search(label) or _LEADING_NUMBER.match(details.get('Arguments', ''))
        if stage:
            query_stage_id = int(stage.group(1))
    return NodeIds(plan_id=plan_id, query_stage_id=query_stage_id)


def expression_ids(texts: Iterable[str]) -> tuple[int, ...]:
    """Each distinct expression id in the texts, in order of first appearance, subquery ids excluded."""
    found: dict[int, None] = {}
    for text in texts:
        subquery_spans = {match.start(1) - 1 for match in _SUBQUERY_REFERENCE.finditer(text)}
        for match in _EXPRESSION_ID.finditer(text):
            if match.start() not in subquery_spans:
                found.setdefault(int(match.group(1)))
    return tuple(found)


def subquery_ids(texts: Iterable[str]) -> tuple[int, ...]:
    """Each distinct subquery id the texts reference: `subquery#N` and `dynamicpruning#N`."""
    found: dict[int, None] = {}
    for text in texts:
        for match in _SUBQUERY_REFERENCE.finditer(text):
            found.setdefault(int(match.group(1)))
    return tuple(found)


def _top_level_arguments(label: str) -> list[str]:
    """Split a label's arguments on the commas outside any brackets or parentheses."""
    arguments = []
    depth = 0
    start = 0
    for index, char in enumerate(label):
        if char in '([':
            depth += 1
        elif char in ')]':
            depth -= 1
        elif char == ',' and depth == 0:
            arguments.append(label[start:index])
            start = index + 1
    arguments.append(label[start:])
    first = _FIRST_TOKEN.match(arguments[0].strip())
    if first:
        arguments[0] = arguments[0].strip()[first.end() :]
    return [argument.strip() for argument in arguments]
