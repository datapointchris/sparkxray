# Event log fixtures

The event logs every test reads. Each variant is one Spark setting that changes what Spark writes,
run over the fixture job in `job/eventlog_job.py`. The plan-shapes variants run
`job/plan_shapes_job.py` instead, one small query per physical plan shape. The plan-rules variants
run `job/plan_rules_job.py`, with one query that fires each `plan.*` rule and one that keeps it
silent. The stage-rules variants run `job/stage_rules_job.py`, with one job seeding each stage,
executor, application and config rule a small cluster can reach. The edited-events variant runs
`job/edited_events_job.py`, then `edit_events.py` edits its log to seed the rules one host cannot.

## Each variant exercises one way Spark writes a log

| Directory under `logs/` | Written by | Exercises |
| --- | --- | --- |
| `spark-3.5.9` | Spark 3.5.9, default log settings | one plain file, committed zstd'd; every seeded rule |
| `spark-4.2.0` | Spark 4.2.0, default log settings | the rolling `eventlog_v2_*` directory, Spark's own zstd, `ResultQueryStage` |
| `spark-3.5.9-lz4` | Spark 3.5.9, `spark.eventLog.compression.codec=lz4` | lz4-java's `LZ4BlockOutputStream` framing |
| `spark-3.5.9-snappy` | Spark 3.5.9, `…codec=snappy` | snappy-java's `SnappyOutputStream` framing |
| `spark-3.5.9-gzip` | Spark 3.5.9, plain, then `gzip -9` | gzip, which only a person or a copy tool produces |
| `spark-4.2.0-rolling` | Spark 4.2.0, rolling at 10 MiB, uncompressed, then zstd'd | many `events_<N>_*` files, read in order of `N` as an integer |
| `spark-3.5.9-killed` | Spark 3.5.9, killed mid-run, then zstd'd | an `.inprogress` name and a truncated final line |
| `spark-4.2.0-killed` | Spark 4.2.0, killed mid-run | an unfinished rolling directory: `appstatus_*.inprogress`, no `ApplicationEnd`, and whole zstd frames ending on a whole line |
| `spark-3.5.9-warmup` | Spark 3.5.9, `--warmup` and skew-join handling on | the second run of a run diff: every stage id shifted, one config change |
| `spark-3.5.9-plan-shapes`, `spark-4.2.0-plan-shapes` | each Spark version, one plain file, then zstd'd | subqueries, reused exchanges, dynamic pruning, a cached relation, a Cartesian product and a Python UDF; plan text cut by `spark.sql.maxPlanStringLength` of 400 and 0, and labels cut past `spark.sql.debug.maxToStringFields` |
| `spark-3.5.9-plan-rules`, `spark-4.2.0-plan-rules` | each Spark version, broadcast joins off, a 6 GiB driver, one plain file, then zstd'd | every `plan.*` rule firing and kept silent, a pandas UDF, and a broadcast over 1 GiB |
| `spark-3.5.9-stage-rules`, `spark-4.2.0-stage-rules` | each Spark version on `local-cluster[2,2,1024]`, two executors of two cores and a 1 GiB heap, with stage executor metrics on and shuffle compression off | an executor lost to an `OutOfMemoryError` and replaced, failed and retried tasks, a failed job, a driver gap, AQE and its skew join turned off for one query, and a standalone `app-*` id |
| `spark-3.5.9-edited-events` | Spark 3.5.9 on `local-cluster[2,2,1024]` with stage executor metrics on, then edited by `edit_events.py` and zstd'd | GC pressure, scheduler delay, deserialization, fetch wait, speculation, executor and host imbalance, oversized shuffle partitions, too few shuffle partitions, and executor and driver heaps used to 97% |
| `glue-5.0` | AWS's Glue 5.0 image (Spark 3.5.4), under Glue's own Spark defaults | a log as Glue's runtime configures it |
| `apache-spark-events` | Apache Spark's own test logs at v4.2.0, zstd'd | a YARN application id, multi-attempt logs ending with no final newline, a standalone app id and older event shapes, and Spark's own rolling directory |

`decoded-by-spark/` holds, for each lz4 and snappy log, the plain lines Spark's own
`CompressionCodec` reads back from it, zstd'd. sparkxray's decoder for that codec must reproduce
them byte for byte.

`history-server-sql/` holds, for each plan-shapes log, what a History Server started over it
serves at `/api/v1/applications/<app>/sql?details=true&planDescription=true`, compacted and zstd'd.
Its graphs must count the same operators as the log's JSON plans, less the query stages.

`explain/` holds, for each plan-rules variant, `cost-mode.json`. It is two queries'
`explain(mode='cost')` text, each beside a description in the job's `fires=` form. No event log
carries the optimizer's size estimates, which `plan.estimate-unreliable` reads. So the job prints
them itself.

Every plain log is committed zstd'd, Apache's included, under the name Spark gives a log it
compressed itself. `local-1` becomes `local-1.zstd`, and `local-1.inprogress` becomes
`local-1.zstd.inprogress`. A plain log is text, so the repo's end-of-file and whitespace hooks
would rewrite its bytes. Compressed, it decompresses to exactly what Spark or Apache wrote. A test
needing the plain file decompresses it into a temporary directory.

The lz4, snappy and gzip variants run two of the jobs only, so each file Spark writes stays under
100 kB.

Spark refuses a rolling file size under 10 MiB, and the size counts the bytes it writes to the
file. With Spark's compression off, ten files take 100 MiB of plain events. So the rolling
variant's padding jobs each carry a 1 MiB local property, which Spark copies into their start
events. One repeated byte compresses to almost nothing, so each rolled file is small once zstd'd.

Spark 4 closes a zstd frame on every flush, so its logs are many frames. A kill therefore lands
between frames, and `spark-4.2.0-killed` decodes to whole lines. A truncated frame is made in a
test by cutting bytes off a fixture.

## Each job's description states what it seeds

Each fixture job sets each job's description to `fires=<rule ids>; silent=<rule ids>; <what it
does>`. Spark copies that description into every stage the job submits, so a test reads its
expectation from the log itself. The `describe(...)` calls in `job/eventlog_job.py` and
`job/plan_rules_job.py` name, for each query, the rules that must fire on it and those that must
stay silent. The cost-mode queries in `write_estimates` do the same, beside their text in
`explain/`.

`glue-5.0` runs the base job under Glue's own defaults, which broadcast the 1,000-row side. Its
description still names `plan.smj-small-side`, so no test reads a plan firing from that log.

`job/stage_rules_job.py` and `job/edited_events_job.py` also name the rules whose findings belong
to the whole run, rather than to one job. They go in the `spark.sparkxray.fixture.application`
property, in the same form, and Spark writes that property into the log's environment.

The rules test holds each source to its descriptions. A finding scoped to a job counts under the
job's description, one scoped to a SQL execution under the execution's, and any other under the
run's property. Every rule under `fires=` must fire there, and none under `silent=` may. Each
source also claims rule families:

- `plan.` for the base logs, the warmup log, the plan-rules logs and the captures;
- `stage.` and `config.` for the stage-rules logs and the edited-events log.

A rule of a claimed family fires on exactly the jobs naming it. The run's property is held only to
the rules it names, because an executor's memory peak depends on when Spark polled it.

## The base log crosses each seeded floor, and the warmup log splits the skew

`stage_totals.py` reads a log with the standard library alone and prints per-stage totals over
successful tasks. A number a test pins comes from here rather than from sparkxray, so a fault in
sparkxray's own reading cannot agree with itself:

```bash
zstd -dc logs/spark-3.5.9/local-*.zstd | python3 stage_totals.py
```

| Stage in `spark-3.5.9` | Tasks | Run time, median / max | Shuffle read, max / median | Records read, max / median | Spilled, memory / disk |
| --- | --- | --- | --- | --- | --- |
| 4, skewed join | 16 | 667 / 25,607 ms | 736,577,381 / 13,611,204 B | 32,536,313 / 506,215 | 6,593,398,048 / 1,816,264,870 B |
| 9, wide sort's map side | 4 | 3,249 / 3,259 ms | 0 | 0 | 796,911,680 / 710,217,271 B |
| 11, wide sort | 4 | 1,651 / 1,654 ms | 189,260,046 / 189,253,825 B | 750,000 / 750,000 | 796,911,680 / 702,452,017 B |
| 20, 200-file write | 200 | 14 / 385 ms | 1,091 / 1,042 B | 100 / 100 | 0 / 0 |

The skewed join's maximum is 54 times its median and 702 MiB, past the 256 MiB floor. The write's
run-time maximum is 27 times its median but under half a second, which the task-skew floor exists
to ignore.

`spark-3.5.9-warmup` runs one extra job first, so every stage id is 6 higher. Its skewed join is
stage 10. Skew-join handling splits it into 19 tasks with a maximum read of 184,510,075 B, under
the 256 MiB floor. Each split still spills far more than the median task. So under skew-join
handling the job describes that query with `stage.spill-skew` firing and `stage.shuffle-skew`
silent.

## The edited-events log changes only the fields each rule reads

Two executors on one host cannot seed some rules. A shuffle block on the same host is fetched
without waiting, and an idle driver schedules a task at once. Spark never places a speculative copy
on a host already running the task. A sleeping task collects no GC time. So `job/edited_events_job.py`
runs one stage shaped for each such problem, and `edit_events.py` edits the plain log Spark wrote:

```bash
python3 edit_events.py < plain-log > edited-log
```

It moves executor 1 to a host of its own, for every event naming its host. Then it edits each job
by the rules its description lists under `fires=`:

| Rule | The edit, on each successful task of the job |
| --- | --- |
| `stage.gc-pressure` | `JVM GC Time` becomes a quarter of `Executor Run Time` |
| `stage.scheduler-delay` | `Finish Time` moves 700 ms later, outside the run time |
| `stage.deserialization` | `Executor Deserialize Time` and `Finish Time` each grow by 700 ms |
| `stage.fetch-wait` | `Fetch Wait Time` becomes 40% of the run time, where the task read a shuffle |
| `stage.speculation` | a task that ran 5 s or more gets a speculative copy on the other executor, launched 1.5 s after it, which succeeds as the original is killed |
| `stage.executor-imbalance` | the first 80% of tasks to launch run on executor 0, the rest on executor 1; with executor 1 on its own host, this also seeds `stage.host-imbalance` |
| `stage.oversized-partitions` | `Remote Bytes Read` grows so the task read 1.1 GiB, where it read a shuffle |

The run's property names `executor.memory-under` and `executor.driver-memory-under`. For each,
the first stage peak of `JVMHeapMemory` becomes 97% of the 1 GiB heap. Accumulator updates stay
as Spark wrote them, because no rule reads them. The editor refuses a log that leaves any of its
edits unnamed, so it never edits another job's log by mistake.

## Regenerating

```bash
fixtures/regenerate.sh                      # every variant
fixtures/regenerate.sh spark-3.5.9-lz4      # one
```

It needs docker, zstd, gzip, curl and jq, and the images `apache/spark:3.5.9-python3`,
`apache/spark:4.2.0-python3` and `public.ecr.aws/glue/aws-glue-libs:5`. Each Spark run takes a
2 GiB driver heap and about a minute. A plan-rules run takes 6 GiB instead. A stage-rules run
takes about three minutes, most of them seeded sleeps, and its out-of-memory seed has one Python
worker build a 1.1 GB string and pickle it. Neither Spark image
ships pyarrow or pandas, so a plan-rules run installs both from PyPI for its pandas UDF. Each run
gets a new application id and new timestamps, so a
regenerated log never matches the committed one byte for byte. Shuffle bytes and records come
from the data, so they do not move between runs. A test pins what the job seeds, never a raw line.
