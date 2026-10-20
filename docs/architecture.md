# Architecture

## A log location becomes findings in four library stages

Each stage reads what the one before it wrote. The CLI calls one library function per command and
holds no logic of its own.

```text
 file, directory, rolling directory, s3:// prefix, History Server URL,
 or the app id of a log already read
        │
        ├── ingest ──→ ordered, decoded event lines
        │
        ├── normalize ──→ Parquet tables + manifest.json, under
        │                 $XDG_CACHE_HOME/sparkxray/tables/<source>/<key>/
        │
        ├── rules ──→ findings: rule id, severity, evidence,
        │             thresholds applied, impact_seconds
        │
        └── reports ──→ Markdown and JSON
```

The tables are keyed by content. The key is a SHA-256 over each ingested file's URI, ETag,
modification time and size, the tables schema version and the installed sparkxray version. So a
second analysis of one run skips parsing, and every upgrade rebuilds the tables. A version bumped
by hand would rebuild less often, and a parser fix that forgot the bump would keep serving the old
parse.

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
Each run installs one of four dependency sets:

- the highest versions;
- the floors the library declares;
- Glue 5.0's preinstalled modules, then the library over them;
- Glue 5.1's, the same way.

A Glue job keeps every preinstalled module that satisfies a floor. So Glue 5.0's numpy 1.26.4 and
pyarrow 17.0.0 are what the library imports there, and neither of the first two sets holds them.

*Rejected:* one distribution with the CLI's dependencies behind an extra. Every CLI install would
have to name the extra, and the CLI would sit on the library's 3.11 floor.

## DuckDB parses the log, so there is no JVM and no custom parser

The benchmark input is a fixture log inflated to 786 MB, holding 126,920 `TaskEnd` events. Over it,
DuckDB's `read_json` with an explicit `columns=` map computed per-stage task aggregates in
0.29–0.30 s on 16 threads. Reading the same log zstd'd took 0.32–0.37 s. On the same file:

- Polars 2.0.0's `scan_ndjson` took 0.36–0.39 s;
- Python with orjson took 2.0–2.1 s.

`benchmarks/parse/README.md` holds every arm and the method.

The explicit `columns=` map is required. Every `SparkListener*` event has its own JSON shape, and
auto-detection unifies them all into one enormous struct. With the map, DuckDB's reader parses in
parallel and drops every field nobody asked for.

*Rejected:* Polars. It parsed in a fifth more time than DuckDB. The rules are SQL over the tables,
and DuckDB is the engine they run in, so Polars would be a second engine beside it.

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
├── s3/<bucket>/<key>/<etag>-<size>     one downloaded object, as S3 holds it
└── decoded/<uri hash>/<version hash>.jsonl   one file's events as plain lines
```

A local file that DuckDB can read as it stands is never copied. The manifest records, per file,
the `read_path` holding its events and the `read_codec` it is compressed with.

A decoded file's version is its URI, ETag, modification time and size, under the installed
sparkxray version. So an upgrade decodes each file again, and never downloads one again.

An entry for an older version of a file is removed by a later open, once it has gone a day
unused. An `EventLog` opened before the file changed still reads its copy. So nothing is removed
the moment a newer version lands. A write goes to a `.part-` file first and is renamed into place.
A partial file a killed process left behind is removed the same way.

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
`tables/<source>/<key>/manifest.json` names the log it was built from. Only the newest entry per
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

A local log Spark is still writing, by its `.inprogress` name or its `appstatus` marker, grows
after it is opened. So its last file is copied into the cache, a plain one at the size it was
listed with. The reader and the table build both read that copy, so neither sees a line Spark
appended later.

Ingest parses only a log's first events and the end of each file. The table build parses every
line, once, and names the file and line of one that does not parse.

## The table build parses every event once, in the order Spark wrote them

`log.tables` builds the tables the first time it is read. DuckDB is imported only then, so code
reading only lines or plans never loads it.

One `read_json` per run of files sharing a codec loads every event into one temporary table,
through the explicit column map. Every table is selected from that copy, so the log is parsed
once. DuckDB's `rowid` keeps the order Spark wrote the events in. AQE's plan updates carry no
timestamp, so that order numbers each query's plan versions.

Each table is written to Parquet with every column cast to the type the registry declares. So any
Parquet reader opens them without sparkxray, and a column's type never depends on what one log
happened to hold.

The build parses every field, each task's accumulator updates included, where the parse benchmark
reads a few task fields. On the benchmark's input and machine, building every table takes
2.0–2.2 s, against the benchmark's 0.3 s. Reopening the cached tables takes 0.02 s.

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

`manifest.json` names what the log was written without, because Spark's defaults leave it out:

| `lacks` | Spark writes it only with |
| --- | --- |
| `stage_executor_metrics` | `spark.eventLog.logStageExecutorMetrics` |
| `block_updates` | `spark.eventLog.logBlockUpdates.enabled` |
| `process_tree_metrics` | `spark.executor.processTreeMetrics.enabled` |

Without stage executor metrics, each task's peaks are the log's only memory figures. So an
executor's `peak_jvm_heap_bytes` is the larger of its stage and task peaks, and
`peak_memory_source` says which it had. Without
process-tree metrics, `peak_python_rss_bytes` is null. Spark writes 0 for a metric it never
measured, and the table does not pass that off as a measurement.

## A column's description sits beside its SQL, so the glossary and `tables list` never disagree

Each registry column carries one line saying what it holds. `tables list` prints that line, and
the metric glossary reads the same one. So no second copy exists to drift from it. The views take
theirs from their sources: `tasks_effective` from `tasks`, and `findings` from the descriptions in
`schemas/findings.json`.

`sparkxray.metric_glossary` adds what the registry cannot say about a number. It covers every
column whose name ends in a unit, and every metric a plan node declares. For each it gives the
unit Spark records beside the unit sparkxray shows, the event fields read, and how a derived
column is computed. Spark-side facts sit in that module, keyed by `table.column`, and a unit
column it lacks raises a `KeyError` whenever the glossary is read.

No meaning is written from a metric's name. Each is quoted from the Spark 4.2 documentation's
tables, or read from the Apache Spark v4.2.0 source, and cites that page or that file and line.
Six metrics only AWS Glue 5.0's Spark build declares have no published source, so they carry no
description. The glossary's metric types match those every fixture log declares.

A name the glossary does not hold is `E-METRIC-001`, listing every column and plan metric it does.

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

Each command calls one function and prints what it returns. The per-resource reads live in
`sparkxray.summaries`, and each runs SQL over the tables into `Rows`. So code importing the library
reads the numbers the command prints, and the command holds no logic to test.

A stage's quantiles take the value at sorted index min(⌊q·n⌋, n−1), as the Stages tab's summary
metrics do. For every stage of the fixtures, each quantile and each total equals what a History
Server serves. Spark summarizes each stage attempt alone, while sparkxray summarizes a stage's
effective tasks across its attempts. So after a retry the two differ.

A stage's wall time runs from its first submission to its last completion, and a SQL execution's
from its start to its end. Each share is that wall time over the run's span. The rules grade
severity by the same shares, from the same SQL, so `stages list` prints the share a finding was
weighed by.

An executor's task time is its tasks' summed duration, which Spark sums too. Spark's Executors tab
then shows the driver's task time as the application's run time, and its GC time as the driver
JVM's own. `task_time_ms` keeps the tasks' sum, because the idle share needs the time tasks held cores. It is
1 − task time × `spark.task.cpus` ÷ (lifetime × cores), since each task holds that many cores.

A SQL execution's operator counts read its last plan version, as the rules do. A stage a job
planned but never ran is what the Spark UI lists as skipped, and no summary holds it.

A query DuckDB refuses is `E-QUERY-001`, carrying DuckDB's message. A stage, SQL execution or
finding the run does not hold is `E-QUERY-002`, and a sort column a list lacks is `E-QUERY-003`.
Each lists the values that would have worked.

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
