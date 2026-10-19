"""The plan parsers agree across sources, and normalization rewrites only ids."""

import gzip
import json
import pathlib
from collections import Counter
from collections.abc import Iterator
from typing import Any

import cramjam
import pytest
from sparkxray.plan import last_plans
from sparkxray.plan import normalize_label
from sparkxray.plan import parse_plan_text
from sparkxray.plan import parse_rest_graph

FIXTURES = pathlib.Path(__file__).resolve().parents[2] / 'fixtures'
PLAN_EVENTS = ('SparkListenerSQLExecutionStart', 'SparkListenerSQLAdaptiveExecutionUpdate')


def _plain_lines(path: pathlib.Path) -> list[bytes]:
    # lz4 and snappy logs are read from the plain lines Spark decoded them to.
    if path.suffix in ('.lz4', '.snappy'):
        path = FIXTURES / 'decoded-by-spark' / path.parent.name / f'{path.name}.jsonl.zstd'
    data = path.read_bytes()
    name = path.name.removesuffix('.inprogress')
    if name.endswith('.gz'):
        data = gzip.decompress(data)
    elif name.endswith('.zstd'):
        data = bytes(cramjam.zstd.decompress(data))
    return data.splitlines()


def _applications() -> Iterator[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]]:
    """Each fixture application's plan events, and its History Server SQL graphs where captured."""
    for variant in sorted((FIXTURES / 'logs').iterdir()):
        for entry in sorted(variant.iterdir()):
            if entry.is_dir():
                parts = sorted(entry.glob('events_*'), key=lambda part: int(part.name.split('_')[1]))
            else:
                parts = [entry]
            events = []
            for part in parts:
                for line in _plain_lines(part):
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue  # a killed run's last line stops mid-write
                    if event['Event'].rpartition('.')[2] in PLAN_EVENTS:
                        events.append(event)
            app_id = entry.name.removeprefix('eventlog_v2_').split('.')[0]
            capture = FIXTURES / 'history-server-sql' / variant.name / f'{app_id}.json.zstd'
            graphs = json.loads(bytes(cramjam.zstd.decompress(capture.read_bytes()))) if capture.exists() else []
            yield f'{variant.name}/{entry.name}', events, graphs


APPLICATIONS_WITH_PLANS = [pytest.param(events, graphs, id=name) for name, events, graphs in _applications() if last_plans(events)]


@pytest.mark.parametrize(('events', 'graphs'), APPLICATIONS_WITH_PLANS)
def test_every_plan_source_counts_the_same_operators(events: list[dict[str, Any]], graphs: list[dict[str, Any]]) -> None:
    plans = last_plans(events)
    assert {plan.source for plan in plans.values()} == {'plan_info_json'}
    # The plan-shapes job names each query's shape. Two cut the plan text short, and `wide` cuts its labels.
    shapes = {event['executionId']: event['description'] for event in events if 'description' in event}
    cut_short = {execution_id for execution_id, shape in shapes.items() if shape in ('max-plan-length', 'max-plan-length-zero')}
    cut_fields = {execution_id for execution_id, shape in shapes.items() if shape == 'wide'}
    assert {execution_id for execution_id, plan in plans.items() if plan.truncated} == cut_short | cut_fields

    last_text = {event['executionId']: event['physicalPlanDescription'] for event in events}
    whole = {execution_id: plan for execution_id, plan in plans.items() if execution_id not in cut_short}
    from_text = {execution_id: parse_plan_text(last_text[execution_id]).operator_counts() for execution_id in whole}
    assert from_text == {execution_id: plan.operator_counts() for execution_id, plan in whole.items()}

    # The History Server's graph holds no query stage, and a cut plan text never cuts it.
    if graphs:
        from_rest = {graph['id']: parse_rest_graph(graph).operator_counts() for graph in graphs}
        assert from_rest == {
            execution_id: Counter(
                {operator: count for operator, count in plan.operator_counts().items() if not operator.endswith('QueryStage')}
            )
            for execution_id, plan in plans.items()
        }


@pytest.mark.parametrize(
    ('label', 'normalized'),
    [
        pytest.param(
            'HashAggregate(keys=[k#13], functions=[count(1)])',
            'HashAggregate(keys=[k#N], functions=[count(1)])',
            id='count(1) keeps its literal',
        ),
        pytest.param('Project [segment#13, n#33L]', 'Project [segment#N, n#N]', id='expression ids'),
        pytest.param(
            'Exchange hashpartitioning(k#4, 16), ENSURE_REQUIREMENTS, [plan_id=160]',
            'Exchange hashpartitioning(k#N, 16), ENSURE_REQUIREMENTS, [plan_id=N]',
            id='plan_id, with the partition count kept',
        ),
        pytest.param(
            'Exchange HashedRelationBroadcastMode(List(cast(input[3, int, false] as bigint))), [id=#32]',
            'Exchange HashedRelationBroadcastMode(List(cast(input[3, int, false] as bigint))), [id=N]',
            id='[id=#] from Spark 3.0',
        ),
        pytest.param('ResultQueryStage 3', 'ResultQueryStage N', id='ResultQueryStage in JSON'),
        pytest.param(
            'ResultQueryStage (20), Statistics(sizeInBytes=8.0 EiB)',
            'ResultQueryStage (N), Statistics(<stats>)',
            id='ResultQueryStage in formatted text',
        ),
        pytest.param(
            'Arguments: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$2082/0x0000000840efd040@75e17c26',
            'Arguments: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda',
            id='$$Lambda class',
        ),
        pytest.param(
            'Filter StartsWith(src#7, s3://raw/2026/)',
            'Filter StartsWith(src#N, <path>)',
            id='a path ends before the closing parenthesis',
        ),
        pytest.param(
            'ReadSchema: struct<file:string,owner:string>, PushedFilters: [EqualTo(owner,ops@cafe.example)]',
            'ReadSchema: struct<file:string,owner:string>, PushedFilters: [EqualTo(owner,ops@cafe.example)]',
            id='a scheme name and an email are literals',
        ),
    ],
)
def test_normalize_label_rewrites_only_ids(label: str, normalized: str) -> None:
    assert normalize_label(label) == normalized
