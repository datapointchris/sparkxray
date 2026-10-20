"""A fixture log's JSON report validates against the report schema, which takes its findings from the findings schema."""

import importlib.resources
import json
import pathlib

import jsonschema
import pytest
import referencing
from fixture_logs import FIXTURES
from sparkxray.ingest import EventLog
from sparkxray.reports import build_report
from sparkxray.reports import render_json


def _schema(name: str) -> dict[str, object]:
    return json.loads(importlib.resources.files('sparkxray').joinpath('schemas', name).read_text())


REPORT_SCHEMA = jsonschema.Draft202012Validator(
    _schema('report.json'),
    registry=referencing.Registry().with_resource('findings.json', referencing.Resource.from_contents(_schema('findings.json'))),
)


# One log per shape a report takes. A codec variant or a Spark 4.2 twin reports what its base log does.
SHAPES = [
    'spark-3.5.9/local-1792406949080.zstd',
    'spark-3.5.9-plan-shapes/local-1792455590972.zstd',
    'spark-3.5.9-stage-rules/app-20261019220120-0000.zstd',
    'spark-3.5.9-edited-events/app-20261019235634-0000.zstd',
    'spark-3.5.9-killed/local-1792407096495.zstd.inprogress',
    'glue-5.0/local-1792407175251.zstd',
    'apache-spark-events/app-20200706201101-0003.zstd',
    'apache-spark-events/local-1430917381535_1.zstd',
]


@pytest.mark.parametrize('log', SHAPES)
def test_each_shape_of_fixture_logs_json_report_validates_against_the_report_schema(log: str, tmp_path: pathlib.Path) -> None:
    report = build_report(EventLog.open(FIXTURES / 'logs' / log, cache_dir=tmp_path))

    REPORT_SCHEMA.validate(json.loads(render_json(report)))
