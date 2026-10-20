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

## Find what slowed a run from a terminal

Every command is `sparkxray <resource> <verb> LOG`. `LOG` is a log file or directory, an `s3://`
URI, or the app id of a log already read. The first command on a log reads it into the cache, and
every later one opens the cache.

```bash
sparkxray findings list s3://your-bucket/spark-logs/spark-application-1729
sparkxray findings show spark-application-1729 stage.shuffle-skew:s4
sparkxray stages list spark-application-1729 --sort wall_ms -n 5
sparkxray tasks list spark-application-1729 --stage 4 --sort duration_ms -n 5
sparkxray tables query spark-application-1729 "select stage_id, max(duration_ms) from tasks_effective group by all"
sparkxray plans show spark-application-1729 --sql 0
sparkxray rules show stage.shuffle-skew
sparkxray metrics show "Executor CPU Time"
sparkxray reports write spark-application-1729 --out report.md
```

A table prints bytes and durations as the Spark UI does, such as `702.5 MiB` and `26 s`. `--json`
prints the raw numbers instead, each under a name ending in its unit, and an empty list as `[]`.
Progress and notes go to stderr, so stdout holds only the data.

`plans show` draws one SQL execution's plan as a tree, with the numbers the Spark UI's SQL tab
shows on its graph. Each node opens with its number, as `n9`, which finding ids and `sql show`
use too. Then it gives what it does, such as a join's type and keys or a scan's location, then the
stages it ran in and its metrics' totals. A node Spark compiled into a codegen pipeline carries the
pipeline's `*(N)`, and the pipeline's `duration` names the nodes it covers. A sort's `spill size`
counts the spills of the nodes it reads from, so the tree states its own part beside it. A node a
finding names carries a mark, as `◀ stage.shuffle-skew (stage 4, high)`. `--initial` shows the
plan the execution started from, before adaptive query execution re-planned it, and
`--plan-version N` any plan between.

`rules show` teaches one rule: what it measures, why it costs time, where the Spark UI shows it
and how to fix it. It takes the same `--threshold` and `--thresholds` overrides `findings list`
does. Its Thresholds table, and any threshold the lesson states, read at the value the rule would
apply. It ends with what the rule found on one of sparkxray's own fixture logs, at the default
thresholds.

`metrics show` says what one number means before anything is read into it. It takes a column, a
plan metric as `plans show` prints it, such as `spill size`, or a Spark field such as
`Executor CPU Time`. Each entry gives the unit sparkxray shows and the unit Spark records, which
differ: Spark writes CPU time and shuffle write time in nanoseconds beside run time in
milliseconds. It also names the event fields read, how a derived column such as
`scheduler_delay_ms` is computed, and where the meaning comes from.
`metrics list` prints every entry, and `tables list` prints every column with what it holds.

A list keeps each row on one line. On a narrow terminal its longest text ends in `…`, and
narrower still each row prints as a block of field and value. A `show` command and `--json`
print every value whole.

`findings list` exits 0 when no finding is at or above `--fail-on`, which defaults to `high`. It
exits 3 when one is, or when the log cannot be read, and 2 on a usage error. So a CI step gates
on the severity it names:

```bash
sparkxray findings list "$LOG" --fail-on critical
```

`--min-severity` and `--limit` narrow what is listed, never what gates. Whenever the log was read,
`--json` carries the verdict as
`"gate": {"fail_on": "high", "passed": false, "blocking": ["stage.shuffle-skew:s4"]}`. So an exit 3
with nothing on stdout means the log was not read.

`reports write` puts one run into one document to read, attach to a ticket or keep. It lists the
findings worst first, each with its evidence, thresholds and fixes folded under it. Then come the
longest stages, the plan of each SQL execution a finding names, and the settings the rules read.
`--format markdown` is the default, and reads on GitHub. `--format json` carries every plan, and
`sparkxray/schemas/report.json` pins its shape. A report holds no timestamp, and prints its times
in UTC. So the same log, thresholds and sparkxray version write the same bytes on any machine.
`--out FILE` replaces the file only once the new one is whole, then prints its path.
`reports write` gates on `--fail-on` as `findings list` does, and writes the report either way.

## Open an event log from a path or S3

`EventLog.open` takes a log file, a directory, a Spark 4 rolling `eventlog_v2_*` directory, or an
`s3://` URI to any of those. The location must hold one application's log. A log already read also
opens by its app id, and `cached_logs()` in `sparkxray.tables.cached_logs` lists those logs, newest
first.

```python
from sparkxray.ingest import EventLog

log = EventLog.open('s3://your-bucket/spark-logs/spark-application-1729')
print(log.manifest.app_id, log.manifest.platform, log.manifest.truncated_tail)
for line in log.lines():
    ...  # one JSON event per line, in the order Spark wrote them
```

It reads uncompressed logs and Spark's zstd, lz4 and snappy. A log compressed by hand reads too,
under the suffix its tool gives it: `.gz` from gzip, `.zst` from zstd. Spark's lzf is refused.
A log Spark is still writing, a killed run's, or a copy cut short can end partway through a line.
What follows the last whole line is left out, and `truncated_tail` says so. A file before the
last one that ends that way is refused. A local log Spark is still writing reads as it stood when
it was opened.

S3 objects are downloaded once into `$XDG_CACHE_HOME/sparkxray/`, or into `cache_dir=`. A second
open of an unchanged object downloads nothing. Credentials come from boto3's default chain, which
`profile=` and `region=` narrow. An S3-compatible store is reached through `AWS_ENDPOINT_URL_S3`.
A copy of an older version of a file stays a day after its last use, so a log opened before the
file changed still reads.

A refusal raises a `sparkxray.errors.SparkxrayError` subclass, whose `code` never changes meaning.
Printed, it gives its message, the values that would have worked, and a hint. `E-INGEST-005`, for
one, is a location holding more than one log, and it lists each.

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
`parse_plan_text` reads `explain()` output. From `explain(mode='cost')` it also reads the
optimizer's size estimates into `plan.estimates`, which no event log carries. `normalize_label`
rewrites a node's ids, so two builds of one query compare equal.

## Query a log's tables with SQL

`log.tables` reads the log into Parquet tables with DuckDB, the first time it is asked. A later
open of the unchanged log reads them from the cache.

```python
from sparkxray.ingest import EventLog

tables = EventLog.open('eventlog.zstd').tables
skew = tables.query(
    'select stage_id, median(shuffle_read_records), max(shuffle_read_records) from tasks_effective group by all'
)
print(skew.fetchall())
tables.tasks.filter('failed').to_arrow_table()   # each table is a DuckDB relation
print(tables.path)                               # the Parquet directory, for any Parquet reader
plans = tables.last_plans()                      # each execution's last Plan, without the log
initial = tables.plan(0, version=0)              # one version of one execution's plan
rows = tables.rows('select * from stages')       # plain Python values, with columns and types
tables.listing()                                 # every table and view: rows, and each column's unit and meaning
```

| Table | One row per |
| --- | --- |
| `application` | the application |
| `environment` | property, by section: `jvm`, `spark`, `hadoop`, `system`, `classpath`, `metrics` |
| `executors` | executor, and the driver |
| `jobs` | job |
| `stages` | stage attempt |
| `tasks` | task attempt, failed, killed and still running included |
| `sql_executions` | SQL execution |
| `plan_versions` | version of an execution's plan, as the event that logged it |
| `plan_nodes` | node of every plan version of every SQL execution |
| `plan_node_metrics` | metric a plan node declares, aggregated as the History Server's SQL tab does |
| `node_stages` | stage a node of an execution's last plan ran in |

Read task metrics through `tasks_effective`. It keeps the first success of each partition of each
stage, so a retried or speculative task counts once, and a stage retry keeps the partitions it did
not rerun. It is `tasks` where `effective` is true, so a Parquet reader filters on that column.
`partition_id` names the partition a task computed, which a retry's `task_index` does not.

A numeric column's name ends in its unit: `_ms`, `_ns`, `_bytes`, `_records` or `_count`. Spark
reports CPU time in nanoseconds beside run time in milliseconds, and the names keep the two apart.
An instant ends in `_epoch_ms`, milliseconds since the epoch. So `launch_epoch_ms` is when a task
started, and `duration_ms` is how long it ran. `plan_node_metrics` holds every kind of metric in
the same columns, so its `unit` column names each row's unit.

`sparkxray.metric_glossary` describes every column with a unit and every plan metric.
`metric_entries('Executor CPU Time')` returns each entry a name matches. Each `MetricEntry` holds
its unit here and in Spark, the event fields it reads, any derivation, and its meaning's source.

`tables.manifest.lacks` names what the log was written without, because Spark leaves it out by
default: `stage_executor_metrics`, `block_updates` or `process_tree_metrics`. A column only
those settings fill, such as `peak_python_rss_bytes`, is null in a log written without them.

`sparkxray.summaries` holds the summaries the command prints, each read from the tables:

| Function | Returns |
| --- | --- |
| `stage_summaries` | one row per stage: tasks, wall time and share of the run, task durations, shuffle, spill, GC |
| `stage_detail` | one stage, and the min, p25, median, p75, p95 and max of every task metric |
| `stage_tasks` | one stage's task attempts |
| `executor_summaries` | one row per executor and the driver: lifetime, cores, memory, task time, idle share |
| `sql_summaries` | one row per SQL execution: duration, jobs, stages, the operators of its last plan |
| `sql_detail` | one SQL execution, its jobs, how many of each operator its last plan holds, and what AQE changed: joins it switched or split for skew, operators it added or removed, and how it read each shuffle |
| `annotated_plan` | one version of one SQL execution's plan, each node with what it does, its metrics and the stages it ran in; `with_findings` adds an analysis's findings on the nodes they name |
| `application_detail` | the application, its duration and its manifest |

A stage's quantiles are taken as the Stages tab takes its summary metrics, so they print the
numbers the Spark UI does.

## Find what slowed a run

`log.analyze()` runs every rule over the log's tables and plans. Each finding names its rule, the
stage, execution or plan node it is about, the evidence, the thresholds it was judged against, and
the seconds a fix could recover.

```python
import json

from sparkxray.ingest import EventLog

log = EventLog.open('eventlog.zstd')
analysis = log.analyze(thresholds={'stage.shuffle-skew': {'ratio': 8}})
for finding in analysis.findings:            # the most recoverable time first
    print(finding.finding_id, finding.severity, finding.impact_seconds, finding.title)
print(analysis.skipped)                      # rules the log held too little to run
print(analysis.gate('high').blocking)        # the findings at high or above, by finding_id
json.dumps(analysis.to_json())               # the shape sparkxray/schemas/findings.json pins
log.tables.findings.filter("severity = 'high'").fetchall()
```

Thresholds also come from a TOML file, `log.analyze(threshold_file=path)`. A size reads as
Spark's own settings do, so `"512m"` and `"512MiB"` are both 536,870,912 bytes:

```toml
["stage.shuffle-skew"]
ratio = 8
min_bytes = "512MiB"
```

`sparkxray.rules.analyze_plan` runs the rules over one plan, such as one read from `explain()`
text, with no log. Only the rules reading a plan alone run, and `skipped` names what each other
rule lacked. A plan printed before it ran has no sizes. After an action,
`explain(mode='formatted')` prints each finished query stage's measured size, which the join rules
read. Simple and cost text print none. `plan.estimate-unreliable` runs on `explain(mode='cost')`
text only. Other code adds its own rules with `sparkxray.rules.register`.

```python
import contextlib
import io

from sparkxray.plan import parse_plan_text
from sparkxray.rules import analyze_plan

printed = io.StringIO()
with contextlib.redirect_stdout(printed):
    df.explain(mode='cost')
for finding in analyze_plan(parse_plan_text(printed.getvalue())).findings:
    print(finding.finding_id, finding.title)
```

`findings.parquet` is written beside the tables, and holds the last analysis's findings.
`log.tables.stored_analysis()` says which rules that analysis ran, under which thresholds, and
which it skipped.

`sparkxray.rules.rule_lesson('stage.shuffle-skew', thresholds=...)` returns a rule's lesson as
Markdown, at the thresholds it would apply, beside the finding it reported on a fixture log.

`sparkxray.reports` builds the document `reports write` prints. A report takes its findings, and
the thresholds they were judged by, from one analysis:

```python
from sparkxray.reports import build_report
from sparkxray.reports import render_report
from sparkxray.reports import write_report

report = build_report(log, analysis)         # without an analysis, log.analyze() at the defaults
write_report(render_report(report, 'markdown'), 'report.md')
report.to_json()                             # the shape sparkxray/schemas/report.json pins
```

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
Spark and stay under the Apache License 2.0, as `fixtures/NOTICE` records. Some of the rule
catalog's wording is adapted from DataFlint, SparkDoctor and Dr. Elephant, and some metric and
column descriptions quote Apache Spark's documentation and source. All four are under the Apache
License 2.0, as `library/NOTICE` records.
