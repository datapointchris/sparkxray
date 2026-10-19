"""The plan parsers agree across sources, and normalization rewrites only ids."""

import gzip
import json
import pathlib
from collections.abc import Iterator
from typing import Any

import cramjam
import pytest
from sparkxray.plan import final_plans
from sparkxray.plan import normalize_label
from sparkxray.plan import parse_plan_text

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


def _applications() -> Iterator[tuple[str, list[dict[str, Any]]]]:
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
            yield f'{variant.name}/{entry.name}', events


APPLICATIONS_WITH_FINAL_PLANS = [pytest.param(events, id=name) for name, events in _applications() if final_plans(events)]


@pytest.mark.parametrize('events', APPLICATIONS_WITH_FINAL_PLANS)
def test_json_and_text_plans_count_the_same_operators(events: list[dict[str, Any]]) -> None:
    last_text = {event['executionId']: event['physicalPlanDescription'] for event in events}
    plans = final_plans(events)
    assert {plan.source for plan in plans.values()} == {'plan_info_json'}
    from_text = {execution_id: parse_plan_text(last_text[execution_id]).operator_counts() for execution_id in plans}
    assert from_text == {execution_id: plan.operator_counts() for execution_id, plan in plans.items()}


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
    ],
)
def test_normalize_label_rewrites_only_ids(label: str, normalized: str) -> None:
    assert normalize_label(label) == normalized
