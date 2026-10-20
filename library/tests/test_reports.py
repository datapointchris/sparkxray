"""A fixture log's JSON report and tables manifest validate against their schemas, which take findings and the ingest manifest by `$ref`."""

import json
import pathlib

import jsonschema
import pytest
import referencing
from fixture_logs import FIXTURES
from sparkxray.ingest import EventLog
from sparkxray.reports import build_report
from sparkxray.reports import render_json
from sparkxray.schemas import schema_document

_REFERENCED = referencing.Registry().with_resources(
    (f'{name}.json', referencing.Resource.from_contents(schema_document(name))) for name in ('findings', 'ingest-manifest')
)
REPORT_SCHEMA = jsonschema.Draft202012Validator(schema_document('report'), registry=_REFERENCED)
TABLES_MANIFEST_SCHEMA = jsonschema.Draft202012Validator(schema_document('tables-manifest'), registry=_REFERENCED)


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
def test_each_shape_of_fixture_logs_report_and_tables_manifest_validate_against_their_schemas(log: str, tmp_path: pathlib.Path) -> None:
    opened = EventLog.open(FIXTURES / 'logs' / log, cache_dir=tmp_path)
    report = build_report(opened)

    REPORT_SCHEMA.validate(json.loads(render_json(report)))
    TABLES_MANIFEST_SCHEMA.validate(opened.tables.manifest.to_json())
