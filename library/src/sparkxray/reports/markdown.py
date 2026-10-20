"""A report as GitHub-flavored Markdown: what to fix first, the evidence folded under each finding, then the run behind it.

It uses no color, emoji or HTML beyond `<details>` and `<summary>`, and writes each severity as a
word, so it reads the same in a plain-text viewer.
"""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from sparkxray.reports.report import Report
from sparkxray.reports.report import SqlSummary
from sparkxray.rules.findings import Finding
from sparkxray.spark_ui_format import shown_in
from sparkxray.summaries.annotated_plans import AnnotatedNode
from sparkxray.summaries.annotated_plans import AnnotatedPlan
from sparkxray.tables.registry import column_unit

_LACKS = {
    'stage_executor_metrics': 'stage executor metrics',
    'block_updates': 'block updates',
    'process_tree_metrics': 'process tree metrics',
}


def render_markdown(report: Report) -> str:
    """The report as Markdown, ending in one newline."""
    sections = [
        _header(report),
        _findings(report.findings),
        _skipped(report),
        _stages(report),
        _sql(report),
        _config(report),
    ]
    return '\n\n'.join(section for section in sections if section) + '\n'


def _header(report: Report) -> str:
    app, usage = report.app, report.usage
    facts = [f'`{app.app_id}`', f'Spark {app.spark_version or "unknown"}', app.platform, shown_in('ms', app.wall_time_ms)]
    if usage.core_hours is not None:
        idle = f', {shown_in("fraction", usage.idle_core_share)} idle' if usage.idle_core_share is not None else ''
        facts.append(f'{usage.core_hours:,.2f} core-hours{idle}')
    if usage.dpu_hours is not None:
        facts.append(f'{usage.dpu_hours:,.2f} DPU-hours')
    lines = [f'# sparkxray report: {_inline(app.app_name or app.app_id)}', '', ' · '.join(fact for fact in facts if fact)]
    caveats = []
    if report.log.truncated_tail:
        caveats.append('its last line was cut, so what followed is missing')
    if report.log.in_progress:
        caveats.append('Spark was still writing it')
    if report.log.compacted:
        caveats.append('a History Server compacted it, dropping finished tasks')
    if report.log.lacks:
        caveats.append(f'Spark wrote it without {_listed([_LACKS.get(lack, lack) for lack in report.log.lacks])}')
    if caveats:
        lines += ['', f'The log: {"; ".join(caveats)}.']
    lines += ['', f'{report.generated_by}, from `{report.log.source}`.']
    return '\n'.join(lines)


def _findings(findings: Sequence[Finding]) -> str:
    if not findings:
        return '## Findings\n\nNo rule found a problem.'
    lines = ['## Findings', '']
    measured = [finding for finding in findings if finding.impact_seconds is not None]
    count = f'{len(findings)} finding{"s" if len(findings) > 1 else ""}.'
    if measured:
        largest = max(measured, key=lambda finding: finding.impact_seconds or 0)
        count += f' The largest single impact is {shown_in("s", largest.impact_seconds)}, from `{largest.finding_id}`.'
        count += ' Findings on one stage can count the same seconds, so the impacts do not add.'
    lines += [count, '']
    lines += _table(
        ('Severity', 'Finding', 'Scope', 'Title', 'Impact'),
        [
            (finding.severity, f'`{finding.finding_id}`', finding.scope.describe(), finding.title, shown_in('s', finding.impact_seconds))
            for finding in findings
        ],
    )
    for finding in findings:
        lines += ['', *_finding_details(finding)]
    return '\n'.join(lines)


def _finding_details(finding: Finding) -> list[str]:
    lines = ['<details>', f'<summary>{finding.severity} · {_html(finding.title)}</summary>', '']
    confidence = finding.confidence + (f', because {finding.confidence_reason}' if finding.confidence_reason else '')
    lines += [f'`{finding.finding_id}` · {_inline(finding.scope.describe())} · confidence {_inline(confidence)}', '']
    if finding.evidence:
        lines += _table(('Evidence', 'Value'), [(f'`{key}`', shown_in(column_unit(key), value)) for key, value in finding.evidence.items()])
        lines.append('')
    if finding.thresholds:
        lines += _table(
            ('Threshold', 'Value', 'Set by', 'Default', 'Default from'),
            [
                (
                    f'`{name}`',
                    shown_in(applied.unit, applied.value),
                    applied.source,
                    shown_in(applied.unit, applied.default),
                    applied.default_source,
                )
                for name, applied in finding.thresholds.items()
            ],
        )
        lines.append('')
    if finding.remediation:
        lines += ['What to try:', '', *(f'{index}. {_inline(step)}' for index, step in enumerate(finding.remediation, start=1)), '']
    if finding.docs:
        lines += ['Read more:', '', *(f'- <{link}>' for link in finding.docs), '']
    lines += [f'How the rule decides: `sparkxray rules show {finding.rule_id}`', '', '</details>']
    return lines


def _skipped(report: Report) -> str:
    if not report.skipped_rules:
        return ''
    rows = [(f'`{skipped.rule_id}`', ', '.join(skipped.missing_signals)) for skipped in report.skipped_rules]
    return '\n'.join(
        [
            '## Skipped rules',
            '',
            'These rules did not run, because the input lacked what they read.',
            '',
            *_table(('Rule', 'Missing'), rows),
        ]
    )


def _stages(report: Report) -> str:
    if not report.stages:
        return ''
    columns = (
        ('Stage', 'stage_id'),
        ('Name', 'name'),
        ('Tasks', 'task_count'),
        ('Wall', 'wall_ms'),
        ('Share', 'wall_share'),
        ('Median task', 'median_task_duration_ms'),
        ('Max task', 'max_task_duration_ms'),
        ('Shuffle read', 'shuffle_read_bytes'),
        ('Spilled to disk', 'disk_spilled_bytes'),
        ('GC', 'gc_share'),
    )
    rows = [tuple(_shown(key, stage[key]) for _, key in columns) for stage in report.stages]
    return '\n'.join(['## Longest stages', '', *_table(tuple(title for title, _ in columns), rows)])


def _sql(report: Report) -> str:
    if not report.sql:
        return ''
    columns = (
        ('SQL', 'execution_id'),
        ('Description', 'description'),
        ('Status', 'status'),
        ('Duration', 'duration_ms'),
        ('Share', 'wall_share'),
        ('Jobs', 'job_count'),
        ('Stages', 'stage_count'),
    )
    rows = [(*(_shown(key, execution.execution[key]) for _, key in columns), str(len(execution.finding_ids))) for execution in report.sql]
    lines = ['## SQL executions', '', *_table((*(title for title, _ in columns), 'Findings'), rows)]
    findings = {finding.finding_id: finding for finding in report.findings}
    for execution in report.sql:
        if execution.finding_ids and execution.plan is not None:
            lines += ['', *_execution_details(execution, execution.plan, findings)]
    if any(not execution.finding_ids for execution in report.sql):
        lines += ['', 'An execution no finding names shows no plan here. The JSON report carries every plan.']
    return '\n'.join(lines)


def _execution_details(execution: SqlSummary, plan: AnnotatedPlan, findings: Mapping[str, Finding]) -> list[str]:
    description = execution.execution.get('description')
    title = f'SQL {plan.execution_id}' + (f' · {description}' if description else '')
    version = f'final plan, version {plan.version}' if plan.is_final else f'last plan, version {plan.version}, not final'
    lines = ['<details>', f'<summary>{_html(title)}: its {version}</summary>', '']
    lines += [f'    {line}' for line in plan_lines(plan, findings)]
    if plan.truncated:
        lines += ['', 'Spark cut this plan short, so a node past the cut is not in it.']
    changes = execution.aqe_changes
    if changes is not None:
        lines += ['', f'AQE re-planned it {changes.update_count} time{"" if changes.update_count == 1 else "s"}.']
        for join in changes.joins:
            what = (
                f'split skewed partitions of {join.final_operator}'
                if join.skew_split
                else f'{join.initial_operator} became {join.final_operator}'
            )
            lines.append(f'- Node {join.final_node_id}: {what}.')
        for operator in changes.operators:
            lines.append(f'- {operator.operator}: {operator.initial_count} in the first plan, {operator.final_count} in the last.')
        for read in changes.shuffle_reads:
            kind = f' {read.kind}' if read.kind else ''
            counts = f', {read.partition_count} partitions from {read.exchange_partition_count}' if read.partition_count is not None else ''
            lines.append(f'- Node {read.node_id}: a{kind} shuffle read{counts}.')
    lines += ['', '</details>']
    return lines


def plan_lines(plan: AnnotatedPlan, findings: Mapping[str, Finding]) -> list[str]:
    """The plan as a tree of plain text: each node's headline and finding marks, then a line of its stages and metrics."""
    lines: list[str] = []

    def draw(node: AnnotatedNode, prefix: str, under: str, named: str) -> None:
        lines.append(f'{prefix}{named}{_node_line(node, findings)}')
        measured = _measured_line(plan, node)
        if measured:
            lines.append(f'{under}{measured}')
        branches = [(plan.nodes[number], '') for number in node.children]
        branches += [
            (plan.nodes[subquery.root], f'subquery {subquery.subquery_id}  ' if subquery.subquery_id is not None else 'subquery  ')
            for subquery in node.subqueries
        ]
        for index, (child, label) in enumerate(branches):
            last = index == len(branches) - 1
            draw(child, under + ('└── ' if last else '├── '), under + ('    ' if last else '│   '), label)

    for node in plan.nodes:
        if node.parent is None:
            draw(node, '', '', '')
    return [line.rstrip() for line in lines]


def _node_line(node: AnnotatedNode, findings: Mapping[str, Finding]) -> str:
    codegen = f'*({node.codegen_stage}) ' if node.codegen_stage is not None else ''
    line = f'n{node.number} {codegen}{node.headline()}'
    runtime = []
    if node.runtime_size_bytes is not None:
        runtime.append(shown_in('bytes', node.runtime_size_bytes))
    if node.runtime_row_count is not None:
        runtime.append(f'{shown_in("rows", node.runtime_row_count)} rows')
    if runtime:
        line += f'  {" · ".join(runtime)} (runtime)'
    for finding_id in node.finding_ids:
        finding = findings[finding_id]
        stage = f'stage {finding.scope.stage_id}, ' if finding.scope.stage_id is not None else ''
        line += f'  ◀ {finding.rule_id} ({stage}{finding.severity})'
    return line


def _measured_line(plan: AnnotatedPlan, node: AnnotatedNode) -> str:
    measured = [f'stage{"s" if len(node.stage_ids) > 1 else ""} {shown_in(None, list(node.stage_ids))}'] if node.stage_ids else []
    measured += [shown for shown in (plan.shown_metric(node, metric) for metric in node.metrics) if shown]
    return ' · '.join(measured)


def _config(report: Report) -> str:
    rows = [(f'`{entry.key}`', f'`{entry.value}`' if entry.value is not None else 'unset (default)') for entry in report.config]
    return '\n'.join(
        ['## Configuration', '', 'The settings the rules reason about, as the run recorded them.', '', *_table(('Key', 'Value'), rows)]
    )


def _shown(key: str, value: Any) -> str:
    return shown_in(column_unit(key), value)


def _table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> list[str]:
    lines = [f'| {" | ".join(headers)} |', f'|{"|".join(" --- " for _ in headers)}|']
    lines += [f'| {" | ".join(_cell(value) for value in row)} |' for row in rows]
    return lines


def _cell(text: str) -> str:
    return _inline(text).replace('|', '\\|')


def _listed(items: Sequence[str]) -> str:
    return items[0] if len(items) == 1 else f'{", ".join(items[:-1])} and {items[-1]}'


def _inline(text: str) -> str:
    return ' '.join(str(text).split())


def _html(text: str) -> str:
    return _inline(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
