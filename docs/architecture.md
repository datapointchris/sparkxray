# Architecture

## Each library stage reads what the one before it wrote

The CLI parses each command's arguments, calls the library and prints what it returns. It holds no
logic of its own.

```text
 file, directory, rolling directory, s3:// prefix, History Server or
 driver UI address, or the app id of a log already read
        │
        ├── ingest ──→ ordered, decoded event lines
        │
        ├── tables ──→ Parquet tables + manifest.json, under
        │              $XDG_CACHE_HOME/sparkxray/tables/<source hash>/<key>/
        │
        ├── enrich ──→ Glue's metrics and insights and a driver's log,
        │              as tables beside them
        │
        ├── rules ──→ findings: rule id, severity, evidence,
        │             thresholds applied, impact_seconds
        │
        └── reports ──→ Markdown and JSON
```

The tables are keyed by the files they were read from, never by those files' bytes. The key is a
SHA-256 over each ingested file's URI, ETag, modification time and size, the tables schema version
and the installed sparkxray version. So a second analysis of one run skips parsing, and every
upgrade rebuilds the tables. The installed version is what rebuilds them after a parser fix. A
version bumped by hand would rebuild less often, and a fix that forgot the bump would keep serving
the old parse.

The schema version is the manifest's JSON contract, `schemas/tables-manifest.json`. A removed
field or table, or a changed meaning, bumps it. An added field, table or column does not.

## The library and the command line are separate distributions

Other code imports the library, including inside AWS Glue jobs. A Glue job importing it must not
pull Typer and rich onto the cluster. So the command line is its own distribution, `sparkxray-cli`,
and the library declares no scripts.

The two also need different Pythons. Glue 5.0 and 5.1 run Python 3.11, so the library supports
3.11. The command line never runs on Glue, and requires 3.13.

Both live in one uv workspace and release from one tag. The CLI's metadata pins
`sparkxray==<its own version>`. So a CLI and a library from different releases cannot install
together.

A uv workspace resolves at one Python: the intersection of its members' `requires-python`. Here
that is 3.13, so nothing in the workspace runs the library at its floor. The
`library-on-python-3.11.yml` workflow does. It installs `./library` unlocked into a 3.11
environment, the way Glue's pip does, then type-checks the library as 3.11 and runs its tests.
Each leg of its matrix installs one of these dependency sets:

- the highest versions;
- the floors the library declares;
- Glue 5.0's preinstalled modules, then the library over them;
- Glue 5.1's, the same way.

A Glue job keeps every preinstalled module that satisfies a floor. So the library imports Glue's
own pyarrow there, as `.github/glue-preinstalled-modules/` records it. Glue 5.0's pyarrow is newer
than the floor and older than the highest release, so neither of the first two sets holds it.

*Rejected:* one distribution with the CLI's dependencies behind an extra. Every CLI install would
have to name the extra, and the CLI would sit on the library's 3.11 floor.

## DuckDB parses the log, so there is no JVM and no custom parser

The benchmark reads a fixture log inflated to 750 MiB, holding 126,920 `TaskEnd` events, and
computes per-stage task aggregates. DuckDB's `read_json` with an explicit `columns=` map was the
fastest arm. It took 7–28% longer to read the zstd'd copy. Polars' `scan_ndjson` took 20–35%
longer than DuckDB. Python with orjson took about seven times as long.
`benchmarks/parse/README.md` holds every arm's timing, the machine, the versions and the method.

The explicit `columns=` map is required. Every `SparkListener*` event has its own JSON shape, and
auto-detection unifies them all into one enormous struct. With the map, DuckDB's reader parses in
parallel and drops every field nobody asked for.

*Rejected:* Polars. It took 20–35% longer than DuckDB over the same file. The rules are SQL over
the tables, and DuckDB is the engine they run in, so Polars would be a second engine beside it.

*Rejected:* a Rust reader. A tuned one might match DuckDB, but only by redoing work DuckDB already
does, and every install would need its build.

*Rejected:* a JVM, including replaying logs through Spark's own History Server. Spark's classes
read logs exactly, but needing Java to read a log on a laptop or in CI is what this tool exists to
avoid. A History Server is an optional input, never a requirement.

## S3 logs are copied to a local cache before DuckDB reads them

DuckDB's Python wheel links `json` and `parquet` statically. Its `httpfs` extension, which reads
`s3://`, is downloaded from `extensions.duckdb.org` the first time a query touches S3. Locked-down
networks block that host, and a Glue job without internet cannot reach it.

So ingest copies objects with boto3 into the cache, and DuckDB only reads local files. An object
is keyed by its ETag plus its size. An unchanged object is never downloaded twice, and a log Glue
is still appending to is downloaded again. One module, `ingest/aws.py`, makes every AWS call.

```text
$XDG_CACHE_HOME/sparkxray/logs/
├── s3/<bucket>/<key>/<etag>-<size>               one downloaded object, as S3 holds it
├── rest/<server hash>/<app>/<attempt>/<version>/  what a server served, or the log rebuilt from it
└── decoded/<uri hash>/
    ├── <version hash>.jsonl                      one file's events as plain lines
    └── <version hash>.json                       whether that version's end was cut
```

A local file that DuckDB can read as it stands is never copied, unless it is the last file of a
log Spark is still writing. The manifest records, per file, the `read_path` holding its events and
the `read_codec` it is compressed with.

## Every cache entry is versioned, and an older version goes once a day unused

A decoded file's version is its URI, ETag, modification time and size, under the installed
sparkxray version. So an upgrade decodes each file again, and never downloads one again.

An entry for an older version of a file is removed by a later open, once it has gone a day
unused. An `EventLog` opened before the file changed still reads its copy. So nothing is removed
the moment a newer version lands. A write goes to a `.part-` file first and is renamed into place.
A partial file a killed process left behind is removed the same way.

## A server's log is downloaded where it serves one, and rebuilt from its REST JSON where not

A History Server serves each application's event log as a zip at
`/api/v1/applications/<app>[/<attempt>]/logs`. sparkxray downloads that first, and reads the
unpacked files as it reads a local log. Their tables match a file ingest's, column for column.

A live driver UI answers the same path with 200 and an empty zip, because only a History Server
writes logs. A server whose log is unreadable answers 503, and an older one 404. In each case
sparkxray rewrites the application's REST JSON as the listener events Spark would have logged,
with `JsonProtocol`'s field names and units, into one file. The one build then reads it, so no
table has a second code path to drift from the first.

Three facts REST carries have no listener event to hold them. The rebuild writes each as an event
of its own, which no Spark writes, and these are the one route a Spark-written log never takes:

- `sparkxray.RestPlanMetrics` carries the SQL tab's metric values, which `plan_node_metrics` reads;
- `sparkxray.RestExecutorPeaks` carries each executor's whole-run peaks, which `executors` reads;
- `sparkxray.RestEvicted` marks a run the server had dropped part of.

A new REST field the tables need goes through one of these, or a new one beside them.

```text
GET <app>/logs ── zip with members ──→ unpack ──→ the files, as a local log
       │
       └── empty zip, 404, 503 ──→ GET environment, allexecutors, jobs, stages, sql,
                                       every started stage's taskList (paged 1000)
                                   ──→ one JSONL of listener events ──→ the same build
```

REST carries less than the log. So the rebuild's manifest names, beside what Spark writes only
when asked, what the build finds missing from the events:

- `stage_parents`: `StageData` holds no parent ids. The simulation then has a stage wait on every
  lower-numbered stage of its job that ended before it was submitted.
- `task_executor_metrics`: no task's executor peaks. Each executor's whole-run peaks come from
  `/allexecutors`. A History Server builds those from the events it replayed. So they count as
  stage peaks where the run set `spark.eventLog.logStageExecutorMetrics`, and as task peaks where
  not, which is what a file ingest of the same log reports.
- `task_accumulators`: no task's accumulator updates. So `node_stages` is empty, and a plan node's
  metrics are the totals and spread the SQL tab displays, read back from their display strings.
  The SQL graph leaves out each query stage the plan text prints. Stepping over those aligns the
  graph's nodes with the text's, and a graph that still differs gives no metrics.
- `execution_configs`: no SQL execution carries the settings it changed, so a setting changed at
  runtime reads as the application's.
- `evicted_records`: the server had dropped part of the run past its `spark.ui.retained*` limits.
  Spark numbers jobs, stages and SQL executions from 0, so a lowest id above 0 shows the oldest
  went. A stage holding fewer tasks than it counts shows some of its tasks went. The rebuild
  writes a `sparkxray.RestEvicted` event, and `app.failed-jobs` needs the `whole_run` signal it
  takes away. A command naming such a log prints a note saying only part of the run is held.

Only each execution's latest plan survives, read from its text. A task's end reason is read back
from the string `TaskEndReason.toErrorString` made of it. A failed job's failure is its failed
stage's, because Spark ends each running stage only that job used with the job's message.

A rule that needs a missing part is skipped by signal. `config.skew-join-off-with-skew` needs
`plan_stages`, which `node_stages` holding rows provides. The rules that read `node_stages` only
to name a node keep running and name none. The two Python UDF rules need only `plan`, so they keep
running too. Their evidence from `node_stages`, the stages that ran the UDF, those stages' share of
task time and the Python workers' peak, comes back empty or null.

A finished attempt's copy is keyed by its end time and last update, so it is fetched once. An
incomplete attempt's download is keyed by its last update. A History Server dates that by the
log's last write, so a run killed long ago is fetched once, and a growing log again as it grows.
A driver UI dates a running attempt by its start. So a rebuild of a running attempt gets a fresh
key on every open, and a live application is never read from the cache. No key holds sparkxray's
version, so an upgrade rebuilds the tables from the cached copy and never fetches again. Downloads
and rebuilds
go under `logs/rest/<server hash>/<app>/<attempt>/<version>/`. One httpx2 client per open bounds
how many requests run at once, and retries a connection error or a 5xx with backoff. It never
retries a 404 or a 503 from `/logs`, which mean no log. `rest_client.py` holds the limits.

## The resolver finds a log by Spark's names, and confirms it by its first line

No platform's folder layout is written into the code. Glue, EMR and a local run each write one of
Spark's two layouts somewhere, and the resolver recognizes the layout:

- a single file `<appId>[.<codec>][.inprogress]`;
- a Spark 4 rolling directory `eventlog_v2_<appId>/`, holding `events_<N>_<appId>[.<codec>]` files
  and an `appstatus_<appId>[.inprogress]` marker.

A rolling log is its files in order of `N` as an integer, so `events_10` follows `events_9`. Glue
leaves the `.inprogress` copy beside the finished log, and the finished one is read.

A log compressed by hand carries its tool's suffix, not Spark's. So `.gz` reads as gzip, and
`.zst` as zstd beside Spark's `.zstd`.

A name only proposes a log. Its decoded first line must be `SparkListenerLogStart`, which Spark
writes first in every log. So a `LICENSE` beside a log is passed over. The first line comes from
a file's first 64 KiB, or its first MiB when that holds no whole line. Candidates are checked
eight at a time, and checking stops once two are logs. So a Glue prefix holding every run of a
job costs one batch of reads.

A History Server can compact a rolling log, replacing its early files with one
`events_<N>_<appId>.<codec>.compact`. Spark's compactor copies every kept line of files 1 to N
into it, in order, so it too starts with `SparkListenerLogStart`. Reading starts at the last
compacted file, and the manifest records `compacted`, because finished tasks were dropped.

A location naming one log that is not a log is `E-INGEST-003`. A directory or prefix holding no
log is `E-INGEST-001`, which lists what it does hold and why each file was passed over. One
holding several logs is `E-INGEST-005`, listing each.

A compressed file under a name that declares no codec is passed over, with the codec its first
bytes show as the reason. Named alone, it is `E-INGEST-003`, and the hint gives the suffix to
rename it to.

A log beside files that start with some other Spark event is `E-INGEST-005` too. Those files are
likely pieces of a log split across several, which sparkxray cannot order. Reading the log alone
would drop them without a word. The caller picks; the resolver never guesses.

## A log already read opens by its app id

An app id never holds a `/`, and an S3 URI or a path with a separator always does. So
`EventLog.open` reads a location with no separator that names nothing on disk as an app id, and
looks for it in the cache.

The cache answers from its tables' manifests alone, so finding a log reads no event. Each
`tables/<source hash>/<key>/manifest.json` names the log it was built from. Only the newest entry per
source counts, whichever sparkxray built it. Entries then merge where they are one log:

- Two locations reaching the same files, such as a directory and the file in it, share a tables
  key.
- A log read while Spark wrote it, then read again after, holds its files under their names
  without `.inprogress`. The later read stands.
- A location that no longer exists loses to one with the same app id that does. Spark renames a
  finished log, so a log read while it ran is looked for under its finished name too.

`cached_logs()` lists what is left, and `sparkxray logs list` prints it. So an app id listed once
opens.

Finished tables the installed sparkxray built open as they stand, so the log's source is never
read. That keeps an S3 log openable with no credentials and no network. A log Spark was still
writing is read again, because it may have grown since, and the tables key reuses its tables if
it has not. Tables an earlier sparkxray built are never read, because their columns may differ
from this one's. Either way the app id resolves to the log's location, and the open reads it as
a path would. So an app id and a path give one answer, and an app id keeps opening after an
upgrade.

An app id no cached log holds is `E-INGEST-001`, listing the app ids the cache does hold. Two
attempts of one application share an app id, and so do two copies of a log that differ. Either is
`E-INGEST-005`, listing each location to open instead.

## Spark's lz4 and snappy logs need two small decoders, and zstd a streaming one

Spark writes a compressed log through its own `CompressionCodec`. Only zstd, Spark 4's default,
writes standard frames that DuckDB opens directly. Spark closes a frame on every flush, so one log
is many frames, and DuckDB reads across them. lz4 writes lz4-java's block stream, and snappy
writes snappy-java's stream. Neither is the standard framing for its codec.

Both are length-prefixed block streams around a standard block codec. So sparkxray carries two
small decoders over cramjam's raw block functions, and decodes those logs into the cache as plain
JSON lines first. lzf is refused with an error that suggests zstd.

zstd is decoded with `zstandard` rather than cramjam. cramjam decodes a zstd buffer whole, and
fails on a frame cut short. `zstandard` decodes one frame at a time and returns every whole block
a cut frame holds. It also reports whether each frame ended, which is how a cut frame is found.

A cut end is expected. Glue's periodic log backups, killed applications and copies interrupted
partway can each leave one. DuckDB fails on a cut last line with `Malformed JSON … unexpected end
of data`. So every file's end is checked before anything reads it:

- a plain file from its last bytes;
- a compressed file by decoding all of it, once per version, with the answer kept in the cache.

A zstd frame or gzip member that never ends is a cut, and so is a last line that does not parse.
A cut last file is decoded or trimmed into the cache without its partial line, and the manifest
records `truncated_tail`. A cut in any other file is `E-INGEST-006`, as is a block that does not
decode.

A local log Spark is still writing grows after it is opened. A single file says so by its
`.inprogress` name. A rolling directory says so by an `appstatus_<appId>.inprogress` marker, or by
having no marker at all, because Spark renames the marker only when the application ends. So its
last file is copied into the cache, a plain one at the size it was
listed with. The reader and the table build both read that copy, so neither sees a line Spark
appended later.

Ingest parses only a log's first events and the end of each file. The table build parses every
line, once, and names the file and line of one that does not parse.

## The table build parses every event once, in the order Spark wrote them

`log.tables` builds the tables the first time it is read. DuckDB and pyarrow are imported then,
or when a log is opened by its app id, because the cache lookup imports them. So code that opens a
log by its location and reads only its lines or plans never loads them.

One `read_json` per run of files sharing a codec loads every event into one temporary table,
through the explicit column map. Every table is selected from that copy, so the log is parsed
once. DuckDB's `rowid` keeps the order Spark wrote the events in. AQE's plan updates carry no
timestamp, so that order numbers each query's plan versions.

Each table is written to Parquet with every column cast to the type the registry declares. So any
Parquet reader opens them without sparkxray, and a column's type never depends on what one log
happened to hold.

The build parses every field, each task's accumulator updates included, where the parse benchmark
reads a few task fields. So building a log's tables takes several times the benchmark's parse, and
reopening the cached tables parses nothing. The first command below times a build on the
benchmark's input, and the second, run after it, a reopen:

```bash
time uv run python -c "from sparkxray.ingest import EventLog; EventLog.open('/tmp/big.json', cache_dir='/tmp/sx-bench').tables"
time uv run python -c "from sparkxray.ingest import EventLog; EventLog.open('/tmp/big.json', cache_dir='/tmp/sx-bench').tables"
```

A build writes into a directory of its own, then renames it into place. Two processes building one
log never read each other's partial files, and the first rename wins. Tables built from an older
version of the log are removed the next time its tables are read, once they have gone a day unused.
So is a build a killed process left partway.

`tasks.effective` marks the first success of each partition of each stage, across the stage's
attempts. `tasks_effective` is `tasks` where it is true. So a retried or speculative task counts
once, while `tasks` keeps every attempt for measuring what retries cost. The selection is a
column, so a Parquet reader of `tasks.parquet` gets it without sparkxray.

A stage retry reruns only the partitions it lost, and numbers its tasks from `Index` 0 again. So
a partition is read from `Partition ID`, which Spark writes from 3.3 on. An older log has only
`Index`, which names a partition within one stage attempt. There a partition a later attempt
reran after it succeeded counts once per attempt.

`plan_node_metrics` counts only each stage's highest attempt instead, because the SQL tab does.
So after a stage retry, its totals can fall short of the same metric summed over
`tasks_effective`.

## The manifest names what the log was written without

`manifest.json` names what the log was written without, so a reader tells a missing value from a
0. The build checks every value on every log, from its events. `LACK_MEANINGS` in
`tables/manifest.py` says what each leaves out, and `sparkxray schemas show tables-manifest` lists
them. Three are what Spark's defaults leave out:

| `lacks` | Spark writes it only with |
| --- | --- |
| `stage_executor_metrics` | `spark.eventLog.logStageExecutorMetrics` |
| `block_updates` | `spark.eventLog.logBlockUpdates.enabled` |
| `process_tree_metrics` | `spark.executor.processTreeMetrics.enabled` |

The rest are what a REST rebuild cannot carry, listed above. An older Spark's own log can lack
them too, as Apache's `app-20200706201101-0003` fixture lacks `execution_configs`.

An executor's `peak_jvm_heap_bytes` is the larger of its stage and task peaks.
`peak_memory_source` is `stage_executor_metrics` wherever the log holds stage peaks for it, and
`task_peaks` where it holds only task peaks. Without stage executor metrics, each task's peaks are
the log's only memory figures. Without process-tree metrics, `peak_python_rss_bytes` is null.
Spark writes 0 for a metric it never measured, and the table does not pass that off as a
measurement.

## A Glue run and a driver's log join as tables beside the log's, after the build

The build reads only the log, so its key is a function of the log's files alone. A Glue run's
metrics need AWS credentials, and a driver log is a file the event log never names. So `enrich`
writes them as tables of their own, into the tables' directory:

```text
tables/<source hash>/<key>/
├── tasks.parquet, stages.parquet, ...   the build, from the log
├── glue_metrics.parquet    ← enrich(glue_run=), from CloudWatch
├── glue_insights.parquet   ← enrich(glue_run=), from CloudWatch Logs
├── driver_log.parquet      ← enrich(driver_logs=), local, S3 or Logs
└── findings.parquet        the last analysis, which reads all of them
```

Each joined table is a signal. A rule reading one runs where it is present and is skipped by name
where not. Giving a source again replaces its table and leaves the others. Every source is read
before any table is written, so a refusal leaves the tables as they were. A later sparkxray
version keys a new directory, whose tables start without them.

Nothing is imported from boto3 unless an AWS source is named. So a local driver log joins with
no AWS libraries loaded.

**Metrics are found by ListMetrics, not by a fixed list of names.** AWS's metric reference spells
`glue.driver.throughput.bytesWrittten` with three t's, and its own Limitations section spells it
with two. ListMetrics filtered on `JobName` and `JobRunId` returns what the run published,
whichever spelling that was. It also returns Glue's older job metrics, which `--enable-metrics`
publishes under the same names. Those carry no `ObservabilityGroup` dimension, so a metric without
one is left out. ListMetrics omits a metric with no data in the last two weeks. So an older run
joins with an empty `glue_metrics` and the `E-ENRICH-003` warning. The analysis still runs,
without the rules that needed it.

Each metric is read at the shortest period CloudWatch still keeps for a run that old: 60 seconds
within 15 days, 300 within 63, and 3600 after. The read starts one period before the application
started and ends two minutes after it ended, because metrics land after a run ends. A log cut
before its end reads to its last event. A log that records no application start has no window, so
it reads none and carries the `E-ENRICH-003` warning. Counts are read as their Sum, as AWS's
reference lists. Throughput gauges, those with a `Source` or `Sink`, are running totals, so they
are read as their Maximum, as AWS's Grafana dashboard reads them. Any other gauge is read as its
Average.
CloudWatch stamps a period with its start, so a datapoint covers `period_start_epoch_ms` to
`period_start_epoch_ms + period_ms`. A rule matches a datapoint to a stage where the two overlap.

AWS gives every percentage metric the unit Percentage and no scale. Its published
`glue.driver.workerUtilization` values run from 0 to 1. Nothing published shows
`glue.driver.memory.heap.used.percentage`'s scale. So `glue_shares_cte` in `tables/registry.py`
reads a run's series as 0 to 100 where any value passes 1. `app.driver-memory-pressure` names the
scale it read as `heap_percentage_scale`.

**Insights are read from `/aws-glue/jobs/logs-v2` only.** That is the continuous-logging group a
job writes to unless it names its own. Each insights stream is named for the run id, so it is read
by name, and a stream that does not exist reads as no events. A run that succeeded writes none.

**A driver log is structured when a record appears in its first 64 KiB.** Spark's structured
logging writes one JSON record a line. A driver's stderr holds more: the JVM's warnings before
log4j starts, and the plain-text trace of an error no logger caught. So the first line is no
test, and a log whose first 64 KiB hold no record is refused as `E-ENRICH-002`. sparkxray numbers
each line as it decodes the log, blank lines included, so `line_number` is the line `rg -n` shows.
DuckDB parses each line that is a JSON object with Spark's record schema, and the table keeps
those with a `ts`.

*Rejected:* DuckDB's `read_json` with `ignore_errors=true`, numbering its rows. It reads a line
that is no JSON as a row of nulls, but a blank line as no row at all. A chained Python traceback
prints blank lines, so every row number after one falls short of its line.

## A column's description sits beside its SQL, so the glossary and `tables list` never disagree

Each registry column carries one line saying what it holds. `tables list` prints that line, and
the metric glossary reads the same one. So no second copy exists to drift from it. The views take
theirs from their sources: `tasks_effective` from `tasks`, and `findings` from the descriptions in
`schemas/findings.json`.

`sparkxray.metric_glossary` adds what the registry cannot say about a number. It covers every
column whose name ends in a unit, and every plan metric the fixture logs declare. For each it
gives the unit Spark records beside the unit sparkxray shows, the event fields read, and how a
derived column is computed. Spark-side facts sit in that module, keyed by `table.column`. A unit
column it lacks raises a `KeyError` whenever the glossary is read. No test reads it, so run
`sparkxray metrics list` after adding a unit column.

No meaning is written from a metric's name. A meaning is quoted from the Spark 4.2 documentation's
tables, read from the Apache Spark v4.2.0 source, or taken from AWS's CloudWatch references. Each
cites that page or that file and line. A column sparkxray reads from one event field cites that
field, under the source `sparkxray, from the event field it reads`. A metric only AWS Glue 5.0's
Spark build declares has no published source, so it carries no description.

A name the glossary does not hold is `E-METRIC-001`, listing every column and plan metric it does.
Where a name is close to one of them, the message ends by asking `did you mean` it.

## Rules read the tables, and a plan alone needs neither DuckDB nor pyarrow

`analyze` reads everything from the tables, plans included. `sql_executions.final_plan_json` holds
each execution's last `sparkPlanInfo`, which `Tables.last_plans()` reads back into a `Plan`.
`plan_versions` holds every version's, and `Tables.plan(execution_id, version)` reads one back. So
analyzing a cached log parses no event line. The findings go into the table directory as
`findings.parquet`, through a `.part-` file renamed into place.

`analyze_plan` runs the rules over one `Plan` and reads no table. Importing `sparkxray.rules` loads
neither DuckDB nor pyarrow, and pyarrow loads only to write `findings.parquet`. So code checking a
plan captured from `explain()` pays for neither.

## The command's summaries are library reads, measured as the Spark UI measures

Each command calls the library and prints what it returns. The per-resource reads live in
`sparkxray.summaries`. Each runs SQL over the tables, and returns `Rows` or a detail value built
from them. So code importing the library reads the numbers the command prints, and the command
holds no logic to test.

A stage's quantiles take the value at sorted index min(⌊q·n⌋, n−1), as the Stages tab's summary
metrics do. Spark summarizes each stage attempt alone, while sparkxray summarizes a stage's
effective tasks across its attempts. So after a retry the two differ.

A stage's wall time runs from its first submission to its last completion, and a SQL execution's
from its start to its end. Each share is that wall time over the run's span. The rules grade
severity by the same shares, from the same SQL, so `stages list` prints the share a finding was
weighed by.

An executor's task time is its tasks' summed duration, which Spark sums too. Spark's Executors tab
then shows the driver's task time as the application's run time, and its GC time as the driver
JVM's own. `task_time_ms` keeps the tasks' sum, because the idle share needs the time tasks held
cores. `idle_share` is 1 − task time × `spark.task.cpus` ÷ (lifetime × cores), since each task
holds that many cores.

A SQL execution's operator counts read its last plan version, as the rules do. A stage a job
planned but never ran is what the Spark UI lists as skipped. No stage summary holds it. `sql show`
lists each job's skipped stages as `skipped_stage_ids`.

A query DuckDB refuses is `E-QUERY-001`, carrying DuckDB's message. It lists the tables only where
the query named one they lack. A query reading `findings` before any analysis wrote them is
`E-RULE-005` instead. A stage, SQL execution, plan version or finding the run does not hold is
`E-QUERY-002`. A stage a job skipped says which job skipped it. A sort column a list lacks is
`E-QUERY-003`. Both list the values that would have worked. Where a name is close to one of them,
the message ends by asking `did you mean` it. A hint that starts `sparkxray` is a command that runs
as written, so a program can run it.

## A report is one value rendered twice, and holds nothing that changes between runs

`build_report` reads one analysis and the tables into a `Report`. `render_markdown` and
`render_json` each print that one value, so the two formats never disagree. Both live in the
library, plan drawing included, so a Glue job writes the same report without the command line's
Typer and rich.

A report carries no timestamp and prints every instant in UTC. It names a local log by its file
name, and each of its files by the path from the directory holding it. It leaves out the tables'
cache key, which hashes each file's modification time. So the same log file, thresholds and
sparkxray version write the same bytes on any machine, and a committed report diffs only where the
run did. `generated_by` names the version, which is what changes the output.

The Markdown shows the plan only of an execution a finding names, folded under `<details>`. A run
with dozens of executions would otherwise bury its findings under trees nobody asked about. The
JSON carries every execution's last plan, because a program reading it chooses for itself.

The core-hours and idle share come from `CORE_TIME_CTES`, the SQL `executor.idle-cores` grades
by. So a report never states an idle share the rule did not judge.

*Rejected:* a timestamp in the report. Two runs on one log would never compare equal.

*Rejected:* drawing plans with rich in both. The library would then import it, and every consumer's
cluster would install it.

## What an agent reads is built from the registries that run sparkxray

`capabilities show` walks the assembled command tree for each command's arguments, flags and exit
codes. It reads the rule catalog, the table registry and the error registry for the rest. The
`tables` and `errors` schemas are built from those registries whenever they are read. So none of
the three has a hand-written copy to fall behind the code, and a command or error added without
docs still appears.

A command's exit codes are read from its parameters. One taking `--fail-on` exits 3 when it fails
the gate that flag's help states. The gate is the flag's to state, because commands gate
different findings: `findings list` every finding, and `logs diff` those run B added or raised to
the bar. One taking a log exits 3 when the log cannot be read. A log parameter shows as `LOG`, or
`LOG_<role>` where a command takes two.

A refusal exits with its error's own status: 2 where the input was wrong, 3 where a person must
act. So a location holding several logs exits 2. A log that does not decode, and a driver log
Spark wrote as plain text, exit 3. `sparkxray schemas show errors` gives each code's status as
`x-exit-code`, and the code says which input failed.

The root group reads whether a command asked for JSON from its words, before any parameter is
parsed: `--json`, or `--format json`. A callback on `--json` would run only once parsing reached
it, so `--fail-on hgih --json` would refuse in prose. The parser's refusal prints as
`E-USAGE-001`, naming the parameter, the values a choice takes, and the command's `--help`.
Without JSON it prints the parser's own text.

The other schemas are files, because their shape is the contract a program holds the document to.
Every object in them is closed. So a key the code adds without its schema fails the test that
validates that document. Each file has a test that validates a document against it, over a
fixture log or the live command.

Every number a schema describes ends in its unit, from the suffixes the finding contract holds
evidence keys to. The exemptions identify rather than measure, as an `_id`, an attempt or a line
number does, and `library/tests/test_schemas.py` lists them. A number beside a `unit` field names
its unit there, as a threshold's `default` and a plan metric's `total` do. One test walks every
schema, the built ones included.

So a plan node's own numbers carry the names `plan_nodes` gives them: `node_id`, `parent_id` and
`codegen_stage_id`. An annotated plan adds `child_ids` beside them. A number naming a node from
elsewhere says whose it is: `plan_node_id` in a finding's scope, `initial_node_id` and
`final_node_id` in an AQE join change, and `root_id` for a subquery's root. The `(3)` formatted
text prints is `operator_id`, Spark's own name for it in `ExplainUtils.generateOperatorIDs`.

*Rejected:* a capabilities document written by hand. An agent trusts it most, and nothing would
say when it went stale.

## A run diff pairs executions and stages by what they ran, never by id

Spark numbers jobs and stages in submission order. One extra action early in a run renumbers
every stage after it. So `diff_logs` pairs run B's SQL executions with run A's in passes, each over
what the passes before left unpaired:

1. the description and the hash of the plan before AQE ran;
2. the description and the operator counts of the last plan;
3. the plan before AQE ran, alone;
4. the description and its order among executions with that description.

Two runs of one query on different data share a first plan, while AQE's last plan follows the
sizes it saw. The third pass is for a description that changed while the query did not, as when a
job describes a query by the settings it ran under. Each pair records the pass that made it, so a
weak pairing reads as weak.

A stage counts toward the execution of the first job listing it, which is the job that ran it. A
later job lists it only to skip it. Inside a paired execution, stages pair by the set of plan
nodes their tasks reported metrics to, then by name. Skew-join handling changes the nodes a join's
stage reports to, so the name pass follows. A stage of no execution pairs by name, which is
Spark's call site. Each log's tables are their own DuckDB connection, so each run is read with SQL
and the pairing runs in Python.

Findings match by rule and the paired part of the run, never by `finding_id`, which carries the
run's ids. A plan node pairs first by its label in the last plan, then in node order, because
skew-join handling rewrites a join's label. Executors have no identity across runs, so they pair
in id order.

A finding of A that B lacks is resolved only where B finished its part. That is a stage that
completed, an execution that ended without an error, or, for the application and its executors,
an application that ended. A B killed partway read a fraction of the data its last stage would
have, too little to show the skew A's did. So its other findings are listed apart, under
`on_parts_b_did_not_finish`, and a finding both report says whether B finished its part.

`--fail-on` gates the findings B added, and each one both runs report that B raised to the bar
from below it. A finding A already had at the bar does not block, so a run that kept a known
problem passes. One that worsened past the bar fails, as `findings list` on B alone would.

An exception becomes a key: its one line, with the run's hosts replaced, then its UUIDs, paths, IP
addresses, hex ids and numbers. Each runs before the next could split what it matches, so a UUID
reads as `<uuid>` rather than as hex runs, and a path as one `<path>`, numbers and all.
`_REWRITES` in `run_diff/exception_triage.py` says why each comes where it does.

A key B shares with a run that succeeded is routine. A fetch failure a retry recovered is one, and
so is a speculative copy killed when its twin finished. A key B shares only with a failed A is
recurring, and one only B holds is new. The view lists new and recurring keys, and counts the
routine ones.

A failed task's exception is in the log. One the driver threw outside a task is not, so the
driver log's exceptions are read beside the tasks', where it was joined. So are a Glue run's
insights. An insight's key leads with Glue's event name, so a task's failure and the job's own
stay apart.

A metric moved notably when its change is 10% of A's value and at least its unit's floor: 5 s of
time, 64 MiB, 1 for a max-to-median ratio, and 10% of A for a count. A share alone is noise on
small numbers, as 40 ms to 80 ms doubles and costs nothing. No prior tool publishes a floor, so
these are proposed.

*Rejected:* pairing by id, which one warmup action breaks for every stage after it.

*Rejected:* one key per exception class. A run that failed on every attempt and a run that
recovered on retry both throw a `PythonException`, and only the line tells them apart.

## The executor simulation replays Sparklens's model, with overlapping jobs replayed together

`sparkxray.simulation` ports Sparklens's executor-count model, because Sparklens needs a JVM.
Each group of jobs that ran together replays onto the executors' task slots. An executor has its
cores over `spark.task.cpus` slots, rounded down. A stage is runnable once every parent with tasks
has finished. A parent the log lacks was skipped, its shuffle output reused, so it counts as done.
Runnable stages start in stage-id order, and each queues every task. With every slot busy, the
task finishing first leaves. A task costs its executor run time, at least 1 ms, as Sparklens
charges it. The time no job ran is the driver's, and every executor count keeps it.

A SQL execution's jobs split into steps that ran one after another, as Sparklens splits them. A job
of no execution is a step alone. A job joins the step before it when it starts before every job of
that step has ended. Sparklens compares it with the job before it alone, so a slow broadcast job
running beside it is missed. Sparklens then replays the steps one after another, which counts a
thread pool's concurrent jobs twice. So steps whose spans overlap replay together, and the
driver's time is the wall time less the groups' spans, which is never negative. Inside a group,
an execution's steps keep their order. Each step waits on the step before it, plus the driver's
time between them, because its jobs read the earlier step's results through the driver. On a run
with no overlap, both models give the same estimate.

A stage counts toward the first job listing it, the job that ran it. Its tasks are its effective
ones, a partition's first success across the stage's attempts, as every rule reads them. Every
other attempt is reported as `retry_run_ms` and never replayed. A group replays every stage it
holds with tasks, rather than walking back from each job's highest stage id as Sparklens does.
That gives the same time on a whole log. On a log cut mid-job, it still replays a job whose
result stage never ran a task.

The model is checked against the run itself. Its estimate at the run's own size against the wall
time is `model_error_share`, and a large one says the curve deserves less trust. Dynamic allocation
is the usual cause, because the model assumes a fixed pool. Stages that read each other in a cycle
raise `E-SIM-001` naming them, where Sparklens returns 0.

`stages list` reads each stage's core-hours over a window from its first task's launch to its end.
Its waste is the core time in that window no task of any stage held. Sparklens opens the window at
the stage's submission and charges it every core its own tasks left. So Sparklens charges a stage
submitted beside a busy one the busy one's core time as its own waste.

*Rejected:* scaling each stage's measured time by the change in cores. A stage's time is set by
its longest task as much as by its total, so halving the executors does not double a skewed
stage, and the replay gets that from the tasks themselves.
