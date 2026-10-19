# sparkxray

Read a Spark event log and find out why a run was slow.

## Install

The command line:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --version     # the installed release
```

Every release is published to that package index, and nothing else is. Name it on every install.
PyPI does not carry sparkxray, so a package of that name there is someone else's.

The library, for code that reads event logs itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ sparkxray
```

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The command
line is a separate distribution, so a project importing the library installs no CLI dependencies.

### Inside an AWS Glue job

Glue 5.0 and 5.1 run Python 3.11, so the library installs into a Glue job. Glue 4.0 runs 3.10 and
cannot. Download the wheel from the index alone, and upload it to S3:

```bash
pip download --no-deps --index-url https://pypi.ichrisbirch.com/simple/ --dest wheels sparkxray
aws s3 cp --recursive wheels/ s3://your-bucket/glue/
```

Then pass its path in the job's parameters, with the version pip downloaded. Glue installs the
wheel, and its dependencies from PyPI:

```text
--additional-python-modules  s3://your-bucket/glue/sparkxray-<version>-py3-none-any.whl
```

Never give pip the package index as an extra index beside PyPI. pip installs the highest version it
finds on either, so anyone uploading a `sparkxray` to PyPI would run their code in the job.

A job with no internet access installs from a zip of wheels instead. Build it on Amazon Linux 2023,
as AWS's guide to a zip of wheels does. Name the downloaded wheel by its path, so pip never asks
PyPI for sparkxray:

```bash
pip download --dest wheels --platform manylinux2014_x86_64 --python-version 311 \
  --only-binary=:all: wheels/sparkxray-<version>-py3-none-any.whl
echo sparkxray > wheels/requirements.txt
zip -r sparkxray-<version>.gluewheels.zip wheels/
```

Upload the zip, pass its `s3://` path to `--additional-python-modules`, and pass `--no-index` to
`--python-modules-installer-option`. Glue then resolves every dependency from the zip.

## Open an event log from a path or S3

`EventLog.open` takes a log file, a directory, a Spark 4 rolling `eventlog_v2_*` directory, or an
`s3://` URI to any of those. The location must hold one application's log.

```python
from sparkxray.ingest import EventLog

log = EventLog.open('s3://your-bucket/spark-logs/spark-application-1729')
print(log.manifest.app_id, log.manifest.platform, log.manifest.truncated_tail)
for line in log.lines():
    ...  # one JSON event per line, in the order Spark wrote them
```

It reads uncompressed logs and Spark's zstd, lz4 and snappy, plus gzip. Spark's lzf is refused.
A log Spark is still writing, or a killed run's, often ends partway through a line. That line is
left out, and `truncated_tail` says so.

S3 objects are downloaded once into `$XDG_CACHE_HOME/sparkxray/`, or into `cache_dir=`. A second
open of an unchanged object downloads nothing. Credentials come from boto3's default chain, which
`profile=` and `region=` narrow. An S3-compatible store is reached through `AWS_ENDPOINT_URL_S3`.

A refusal raises a `sparkxray.errors.SparkxrayError` subclass, whose `code` never changes meaning.
`E-INGEST-001`, for one, is a location holding no event log, and its message lists what it holds.

## Read a query's physical plan

`sparkxray.plan` reads a plan from an event log, from `df.explain()` text in simple or formatted
mode, or from a History Server's SQL graph. Every form reads into one tree of nodes.

```python
import json

from sparkxray.ingest import EventLog
from sparkxray.plan import last_plans

events = [json.loads(line) for line in EventLog.open('eventlog.zstd').lines()]
for execution_id, plan in last_plans(events).items():
    print(execution_id, plan.is_final, plan.operator_counts().most_common(3))
```

`last_plans` keeps each SQL execution's last plan, after adaptive query execution re-planned it.
For an execution that never finished, as in a killed application, `is_final` is False.
`parse_plan_text` reads `explain()` output. `normalize_label` rewrites a node's ids, so two builds
of one query compare equal.

## Layout

| Directory | Holds |
| --- | --- |
| `library/` | the `sparkxray` distribution, which code reading event logs imports |
| `cli/` | the `sparkxray-cli` distribution: the `sparkxray` command over the library |
| `docs/` | why the design is what it is |
| `fixtures/` | the event logs the tests read, and the Spark job and script that write them |
| `benchmarks/parse/` | the parse benchmark behind the choice of DuckDB |

Both distributions are released together, from one tag, and the CLI pins the library to its own
release.

## License

MIT. The Apache Spark test logs under `fixtures/logs/apache-spark-events/` are copied from Apache
Spark and stay under the Apache License 2.0, as `fixtures/NOTICE` records.
