# sparkxray

Read a Spark event log offline, with no JVM, and find out why a run was slow.

Spark logs every job, stage and task a run executed. sparkxray reads that log from a file, S3 or a
History Server, and lists what slowed the run. A finding names a skewed partition, a spill, a join
that shuffled a side it could have broadcast, or cores left idle. Each one carries its evidence, the
seconds a fix could recover, and the fix. The same reading is a Python library, so a job or a test
can check its own run.

## Install

The command line:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --version     # prints the installed release
```

Every release is published to that package index, and nothing else is. Name it on every install.
PyPI does not carry sparkxray, so a package of that name there is someone else's.

The library, for code that reads event logs itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ sparkxray
```

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The command
line is a separate distribution, so a project importing the library installs no CLI dependencies.
An AWS Glue job installs the library from a wheel, as
[Run the library inside an AWS Glue job](#run-the-library-inside-an-aws-glue-job) shows.

## Get an event log

Spark writes an event log when `spark.eventLog.enabled=true`, into the directory
`spark.eventLog.dir` names. Before Spark 4.0 it writes one uncompressed file per application. From
4.0 it writes a directory of zstd'd files, named `eventlog_v2_` and the app id. sparkxray reads
either, and a log compressed by hand.

A History Server reads the same directory, and sparkxray takes the server's address too. An AWS
Glue job writes its log to S3 when it runs with these job parameters:

```text
--enable-spark-ui        true
--spark-event-logs-path  s3://your-bucket/spark-logs/
```

## Find what slowed a run from a terminal

```bash
sparkxray findings list ./eventlog.zstd                        # what slowed the run, the most time a fix could recover first
sparkxray findings show ./eventlog.zstd stage.shuffle-skew:s4  # one finding: its evidence, its thresholds and how to fix it
sparkxray rules show stage.shuffle-skew                        # what the rule measures, why it costs time, where the Spark UI shows it
sparkxray plans show ./eventlog.zstd 0                         # the query's plan, with the finding marked on the node it names
sparkxray logs list                                            # every log read so far, by the app id each command also takes
```

The first command lists every finding with its id, then prints the `findings show` command for the
first. The ids above are from one of sparkxray's own test runs, so yours will differ. That run has
high findings, so `findings list` exits 3 on it.

`LOG` is a log file or directory, an `s3://` URI, a History Server's or driver UI's `http(s)://`
address, or the app id of a log already read. The first command on a log reads it into the cache
under `$XDG_CACHE_HOME/sparkxray/`. Every later command opens the cache rather than the log.
Deleting that directory empties the cache.

A command is `sparkxray <resource> <verb>`, and one that reads a run takes `LOG` first. `list`
prints a set and `show` prints one. So `stages list`, then `stages show LOG 4`, then
`tasks list LOG --stage 4` walks from the run down to one stage's tasks. `sql show LOG ID`, then
`plans show LOG ID`, does the same for a query. `sparkxray --help` lists every resource. A
command's `--help` says what it takes, what each column means, and what it prints.

Beside the findings, sparkxray does these:

```bash
sparkxray reports write LOG --out report.md    # the findings and the run behind them, as one Markdown document
sparkxray logs diff GOOD_LOG SLOW_LOG          # what changed in a slow run against a good run of the same job
sparkxray executors simulate LOG               # the run's wall time at other executor counts
sparkxray tables query LOG "SELECT ..."        # DuckDB SQL over the run's tables
sparkxray logs ingest LOG --glue-run JOB_NAME:RUN_ID --driver-log FILE   # join a Glue run's metrics and the driver's own log
```

A table prints bytes and durations as the Spark UI does, as `702.5 MiB` and `26 s`. It names each
column as `--json` does, with spaces for underscores. `--json` prints the raw numbers, each under a
name ending in its unit, and an empty list as `[]`. Progress and notes go to stderr, so stdout
holds only the data. A table fits the terminal by cutting its longest text with `…`. On a narrow
terminal each row prints as a block of field and value. `--json` prints every value whole.

A finding's severity is `info`, `low`, `medium`, `high` or `critical`. Its rule grades it, then
moves it a step by the share of the run its stage or query took. `findings list` exits 0 when no
finding is at or above `--fail-on`, which defaults to `high`. It exits 3 when one is, or when the
log cannot be read, and 2 on a usage error. `reports write` gates the same way, and `logs diff`
does when given `--fail-on`. So a CI step gates on the bar it names:

```bash
sparkxray findings list "$LOG" --fail-on critical
```

Whenever the log was read, `--json` carries the verdict as
`"gate": {"fail_on": "high", "passed": false, "blocking": ["stage.shuffle-skew:s4"]}`. So an exit 3
with nothing on stdout means the log was not read.

## A program or an agent drives sparkxray from what it prints

`sparkxray capabilities show --json` lists every command with its arguments, flags and exit codes,
every rule with its thresholds, and the tables, schemas and error codes. It is read from the
running command and library, so it names a command or rule no page documents yet.

`sparkxray schemas list` names every JSON Schema sparkxray ships, and `sparkxray schemas show NAME`
prints one. `tables` gives each table's columns with their DuckDB types and units. `errors` gives
the error envelope, and every code with its exit status. Every number any schema describes ends
in its unit, as `_ms` or `_bytes`, or sits beside a `unit` field.

Given `--json`, or `--format json` on `reports write`, a refusal prints one line of JSON on
stderr, and the command exits with the error's status. A command line the parser refuses, such as
`--fail-on hgih`, prints as `E-USAGE-001` and exits 2. A code is never reused or renumbered, so a
program branches on it. `field` names what it passed that failed, and `valid` what would have
worked there. A `hint` that starts `sparkxray` is a command that runs as written:

```json
{"error": {"code": "E-RULE-002", "message": "stage.shuffle-skew has no threshold called 'ratoi'; did you mean ratio?", "field": "threshold.stage.shuffle-skew.ratoi", "valid": ["ratio", "min_bytes", "min_tasks"], "hint": "sparkxray rules show stage.shuffle-skew"}}
```

## Open an event log

`EventLog.open` takes a log file, a directory, a Spark 4 rolling `eventlog_v2_*` directory, an
`s3://` URI to any of those, or a Spark server's `http(s)://` address, described below. The
location must hold one application's log. A log already read also opens by its app id, and
`cached_logs()` in `sparkxray.tables.cached_logs` lists those logs, newest first.

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

`EventLog.open` also takes the address of a Spark History Server or a live driver UI. That can be
its root, an application's page, or its REST API address with an attempt id. `app=` picks one
application where the server holds several, and `sparkxray logs ingest URL --app APP_ID` does the
same.

sparkxray downloads a History Server's log where the server serves one. A driver UI serves none,
and a History Server whose log is unreadable answers 404 or 503. sparkxray then rebuilds the log
from the application's REST JSON. The rebuild lacks each stage's parents, each task's accumulator
updates and executor peaks, and the settings a query changed. It also lacks whatever the server
dropped past its `spark.ui.retained*` limits. The manifest's `lacks` names each. A rule reading
which stages ran a plan node is then skipped, and so is `app.failed-jobs` where the server dropped
part of the run.

```python
log = EventLog.open('http://localhost:18080', app='spark-application-1729')
log = EventLog.open('http://driver-host:4040')  # a running application, rebuilt afresh on every open
```

A refusal raises a `sparkxray.errors.SparkxrayError` subclass, whose `code` never changes meaning.
Printed, it gives its message, the values that would have worked, and a hint. `E-INGEST-005`, for
one, is a location holding more than one log, and it lists each.

## Find what slowed a run from Python

`log.analyze()` runs every rule an event log can feed over the log's tables and plans. Each finding
names its rule, the stage, execution or plan node it is about, the evidence, the thresholds it was
judged against, and the seconds a fix could recover.

```python
import json

from sparkxray.ingest import EventLog

log = EventLog.open('eventlog.zstd')
analysis = log.analyze(thresholds={'stage.shuffle-skew': {'ratio': 8}})
for finding in analysis.findings:            # the most recoverable time first
    print(finding.finding_id, finding.severity, finding.impact_seconds, finding.title)
print(analysis.skipped)                      # rules the log held too little to run
print(analysis.gate('high').blocking)        # the findings at high or above, by finding_id
json.dumps(analysis.to_json())               # the shape `sparkxray schemas show findings` prints
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

`sparkxray.rules.rule_lesson('stage.shuffle-skew', thresholds=...)` returns a `RuleLesson`. Its
`text` is the lesson as Markdown, at the thresholds the rule would apply. Its `example` is what the
rule found on one of sparkxray's test runs.

`sparkxray.reports` builds the document `reports write` prints. A report takes its findings, and
the thresholds they were judged by, from one analysis:

```python
from sparkxray.reports import build_report
from sparkxray.reports import render_report
from sparkxray.reports import write_report

report = build_report(log, analysis, fail_on='critical')  # without an analysis, log.analyze() at the defaults
write_report(render_report(report, 'markdown'), 'report.md')
report.gate.passed                           # False when a finding is at or above fail_on
report.to_json()                             # the shape `sparkxray schemas show report` prints
```

`sparkxray.run_diff.diff_logs` compares two runs as `logs diff` does:

```python
from sparkxray.run_diff import diff_logs

diff = diff_logs(EventLog.open(good), EventLog.open(slow), baselines=[EventLog.open(other_good)])
[finding.finding_id for finding in diff.findings.new]
diff.gate('high').passed                     # False when the slow run added a finding at or above high, or raised one to it
diff.to_json()                               # the shape `sparkxray schemas show diff` prints
```

`sparkxray.simulation.simulate` estimates a run at other executor counts as `executors simulate`
does:

```python
from sparkxray.simulation import simulate

simulation = simulate(EventLog.open(location), executors=[4, 8, 16])
[(point.executor_count, point.estimated_wall_ms) for point in simulation.curve]
simulation.model_error_share                 # how far the estimate at the run's own size is from its wall time
simulation.to_json()                         # the shape `sparkxray schemas show simulation` prints
```

`sparkxray.schemas.schema_document(name)` returns any schema `schemas show` prints, and
`sparkxray.errors.registered()` every error class, each with its `code`, `exit_code` and
`raised_when`. A `SparkxrayError`'s `to_json()` is the envelope's `error`.

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

`tables.listing()`, or `sparkxray tables list LOG`, names every table and view with its row count,
and each column's type, unit and meaning.

Read task metrics through `tasks_effective`. It keeps the first success of each partition of each
stage, so a retried or speculative task counts once, and a stage retry keeps the partitions it did
not rerun. It is `tasks` where `effective` is true, so a Parquet reader filters on that column.
`partition_id` names the partition a task computed, which a retry's `task_index` does not.

A column holding a measurement ends in its unit, as `_ms`, `_ns` or `_bytes`. Spark records a
task's CPU time in nanoseconds beside its run time in milliseconds, and the names keep the two
apart. An instant ends in `_epoch_ms`, milliseconds since the epoch. So `launch_epoch_ms` is when a
task started, and `duration_ms` is how long it ran. An id has no unit. `plan_node_metrics` and
`glue_metrics` hold every kind of metric in the same columns, so each one's `unit` column names a
row's unit. `sparkxray schemas show tables` gives every column's unit.

`sparkxray.metric_glossary` describes every column with a unit, and the plan metrics Spark's own
operators declare. `metric_entries('Executor CPU Time')` returns each entry a name matches. Each
`MetricEntry` holds its unit here and in Spark, the event fields it reads, any derivation, and its
meaning's source.

`tables.manifest.lacks` names what the log was written without, so a missing value never reads as
0. Spark leaves three out by default: `stage_executor_metrics`, `block_updates` and
`process_tree_metrics`. A column only those settings fill, such as `peak_python_rss_bytes`, is
null in a log written without them. The rest are what a log rebuilt from REST JSON goes without,
and an older Spark's log may too. `sparkxray schemas show tables-manifest` lists each value and
what its absence means.

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
Under adaptive query execution, an execution that never finished, as in a killed application, has
`is_final` False. A plan run without it is final as written. `parse_plan_text` reads `explain()`
output. From `explain(mode='cost')` it also reads the optimizer's size estimates into
`plan.estimates`, which no event log carries. `normalize_label` rewrites a node's ids, so two
builds of one query compare equal.

## Join a Glue run's metrics and the driver's own log

An event log says little about the driver. A driver that ran out of memory can leave a log whose
last job succeeded. Two sources beside the log say more. AWS Glue publishes a job run's metrics to
CloudWatch, and a failed run's job run insights to CloudWatch Logs. Spark 4.0 and later write the
driver's log as JSON records under `spark.log.structuredLogging.enabled=true`.

`logs ingest` joins either to a log's tables:

```bash
sparkxray logs ingest s3://your-bucket/spark-logs/spark-application-1729 \
  --glue-run nightly-orders:jr_0123abcd \
  --driver-log s3://your-bucket/driver-logs/stderr.log.gz
```

`--glue-run JOB_NAME:RUN_ID` reads the run's metrics over the window the application ran in. The
job must have run with `--enable-observability-metrics true`. The older job metrics that
`--enable-metrics` publishes are not read. CloudWatch lists no metric silent for two weeks, so an
older run joins without them, and `logs ingest` says so. It also reads the run's insights from
`/aws-glue/jobs/logs-v2`, which a job writes only when it fails with `--enable-job-insights true`.

`--driver-log` takes a local file, an `s3://` object, or a CloudWatch Logs stream as
`cloudwatch:GROUP:STREAM`, and repeats. A file may be gzip'd or zstd'd. Lines that are no JSON
record, such as the JVM's own warnings, are skipped. A log with no record in its first 64 KiB is
refused as `E-ENRICH-002`, because Spark wrote it as plain text.

Reading needs `cloudwatch:ListMetrics`, `cloudwatch:GetMetricData` and `logs:GetLogEvents`. A
driver log on S3 also needs `s3:ListBucket` and `s3:GetObject`. Nothing is written to AWS. The
region comes from `AWS_DEFAULT_REGION` or the profile, as boto3 reads it.

```python
from sparkxray.enrichment import GlueRun

enrichment = log.enrich(glue_run=GlueRun('nightly-orders', 'jr_0123abcd'), driver_logs=['stderr.log'])
enrichment.row_counts     # the rows each table now holds
enrichment.warnings       # E-ENRICH-003 where CloudWatch listed no observability metric for the run
```

| Table | One row per |
| --- | --- |
| `glue_metrics` | CloudWatch datapoint: one metric's statistic over one period |
| `glue_insights` | event of a failed run's exception analysis or rule-based root cause |
| `driver_log` | JSON record of every driver log given |

Joined tables feed the rules:

- `app.driver-oom` reports an `OutOfMemoryError` in the driver log.
- `app.driver-memory-pressure` reports the driver's heap held at least 90% full for two minutes
  or more.
- `stage.task-skew` adds Glue's own skewness score to its evidence.
- `executor.idle-cores` adds Glue's worker utilization to its evidence.

`logs diff` reads the driver's and Glue's exceptions beside the tasks'. Giving a source again
replaces its table. A later sparkxray version rebuilds the tables without them, so ingest with the
flags again.

## Run the library inside an AWS Glue job

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
catalog's wording is adapted from DataFlint, SparkDoctor and Dr. Elephant. Some metric and column
descriptions quote Apache Spark's documentation and source. The executor simulation ports
Sparklens's model. Each is under the Apache License 2.0, and `library/NOTICE` names each project
and what came from it.
