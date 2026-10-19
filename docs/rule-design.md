# Rule design

## Every ratio threshold is paired with an absolute floor

A ratio alone fires on noise. In the `spark-3.5.9` fixture, the 200-file write's stage runs 200
tasks with a 14 ms median and a 385 ms maximum. That is a 27× max-to-median duration ratio, and
nothing there is worth fixing.

Durations also understate real skew. The same fixture's skewed join reads 64× the median's shuffle
records on its largest task, but runs only 38× the median's time. `fixtures/README.md` gives both
stages' totals and the command that reads them.

So every ratio comes with an absolute amount, the way AQE's own skew test pairs
`skewedPartitionFactor` (5) with `skewedPartitionThresholdInBytes` (256 MB). Stage rules lead with
bytes and records, and use duration as a secondary signal. `stage.task-skew` needs a gap of at
least 30 s between the slowest and the median task, which keeps it silent on the 200-task stage.

## Severity is the rule's grade, moved one step by the stage's share of the run

The ladder is `info`, `low`, `medium`, `high`, `critical`. `info` is for rules that report
presence only. A rule grading a plan shape by its size is `info` below its byte floor too, and
wherever nothing measured the size. So a window over 30 aggregated rows reports without gating a
run.

Each rule's own ladder sets the base grade. The engine then lowers it one step when the scope
takes under 5% of application wall time, and raises it one step above 50%. So a skewed 2-second
stage does not outrank a balanced 2-hour one. `info` never moves, and nothing drops below `low`.

A stage is weighed by its own wall time, and a SQL execution or plan node by its execution's. An
application or executor finding has no share and keeps its grade. The finding keeps the rule's
grade as `base_severity`, and the share as `stage_wall_share` or `sql_execution_wall_share` in its
evidence, so the adjustment is visible.

A run still going when its log ended has no end time. It ends at the last task, stage, job or
execution the log saw finish. A stage left open ends at its last task, and an execution left open
ends with the run.

## The gate judges every finding, whatever the listing shows

`Analysis.gate(fail_on)` names each finding at or above a severity, by `finding_id`. `findings
list` exits 0 when none is, 3 when one is or the log cannot be read, and 2 on a usage error. So a
CI job gates on the severity it names, and a scheduled report stays green while it lists medium
findings.

A failed gate and an unreadable log share exit 3, because each needs a person to look. Neither is
a difference the tool could repair on its own, which is what a separate exit 1 would mean.

The gate is judged before `--min-severity` and `--limit` narrow the list. So `-n 0` prints no
finding and still exits 3 on a high one. A gate judged after narrowing would pass a run by hiding
what failed it.

`--json` carries the gate beside the findings whenever the log was read, as the findings schema's
optional `gate`. A caller reads the verdict there rather than inferring it from the exit status.
An exit 3 with no `gate` on stdout means the log was not read. A severity outside the ladder is
`E-RULE-006`, listing the five.

## `impact_seconds` is wall time a fix could recover, and null where nobody can say

| Rule | `impact_seconds` |
| --- | --- |
| `stage.task-skew` | the slowest task's run time minus the median task's |
| `stage.shuffle-skew`, `stage.spill-skew` | the slowest task's duration minus the median task's |
| `stage.spill` | the spilling tasks' run time × (disk spill ÷ (shuffle read + input)), capped at stage wall time |
| `stage.tiny-tasks` | the tasks' summed duration less their run time, ÷ the stage's concurrent slots |
| `stage.scheduler-delay` | the summed scheduler delay ÷ concurrent slots |
| `stage.deserialization` | the summed deserialization time ÷ concurrent slots |
| `stage.retry-waste` | the failed attempts' run time ÷ concurrent slots |
| `app.driver-gaps` | the gaps' summed length |
| every other rule | null |

A stage's concurrent slots are its task count or the cores of the executors that ran its tasks,
whichever is fewer.

Findings sort by `impact_seconds`, largest first and nulls last, then by severity.

## A rule declares the signals it reads, so it runs wherever they exist

The signals are `plan`, `plan_metrics`, `plan_estimates`, `tasks`, `executors`, `environment` and
`timeline`. A rule whose signals are missing is skipped, and the skip names the missing signal. So
`plan.*` rules run on a bare `explain()` plan with no event log, and a skipped rule says what it
would have needed.

`plan_estimates` is the optimizer's size estimates. Only `explain(mode='cost')` text prints them,
and no event log carries them. So `plan.estimate-unreliable` runs through `analyze_plan` alone.
`analyze` leaves it out of a log's default selection, so a log's analysis neither runs it nor
lists it as skipped. Named with `rules=`, it is skipped, and `findings list` says it runs on
cost-mode text only.

Rule ids are dotted by the evidence they read:

| Prefix | Reads |
| --- | --- |
| `plan.` | plan nodes, and their metrics where present |
| `stage.` | effective tasks and stages |
| `executor.` | executors, their memory peaks and their memory settings |
| `app.` | jobs, stages and executors as a timeline |
| `config.` | the environment, against other findings |

## A rule may read another rule's findings, and that rule runs first

A Python rule names the rules whose findings it reads in `reads_findings_of`. Its context's
`findings` holds them by rule id. Selecting the rule selects those it reads, and they run first
and report as any rule does. Where one of theirs lacks a signal, the reading rule skips too.

Only a Python rule may read findings, and no rule reads its own. `register` refuses a rule reading
one the catalog does not hold. So the order is always known before the first rule runs.

Two rules read findings:

- `stage.oversized-partitions` leaves out the stages `stage.shuffle-skew` found. A skewed
  partition is too large because it is skewed, and the skew finding names the fix.
- `config.skew-join-off-with-skew` fires on each `stage.shuffle-skew` finding whose stage ran a
  sort-merge or shuffled hash join, where AQE's skew join was off for that execution.

*Rejected:* reading the findings table from SQL. It holds the last analysis, which need not have
run the rule read, or run it with the same thresholds.

## A SQL rule reads its thresholds as parameters, and a Python rule reads the plans

A SQL rule runs in DuckDB over the tables, and reads each threshold as a `$name` parameter. DuckDB
refuses a parameter a query never reads, so a `Rule` whose SQL does not name each of its thresholds
is refused when it is built. A Python rule's `check` receives each execution's last plan, numbered
as `plan_nodes` numbers it, with each node's metric totals and the tables.

Either returns one mapping per finding, and the engine sorts its keys three ways:

- `severity`, `impact_seconds`, `confidence` and `confidence_reason` fill the finding's own fields;
- the scope ids fill `scope`, and a plan node's label is read from the plan where a rule gives none;
- every other key is evidence.

A numeric evidence key must end in its unit, such as `_bytes`, `_ms`, `_records`, `_rows`,
`_count`, `_share` or `_ratio`. A list of node or stage ids ends in `_ids`. The engine refuses a finding that breaks this, and the findings
schema does too. So a rule registered by other code is held to the same shape. A malformed rule,
a taken id, or a finding that breaks the contract is `E-RULE-004`, naming the rule.

A rule's `title` is a `str.format` template over the finding's scope ids and evidence. In it,
`{key:bytes}` prints a byte count as the Spark UI's `Utils.bytesToString` does, `702.5 MiB`, and
`{key:duration}` prints milliseconds as its `UIUtils.formatDuration` does, `26 s`. So a title reads
the way the Stages tab shows the same number.

## A finding is named by its rule and its scope

`finding_id` is the rule id and a key built from the scope's ids: `app` for the application, `e0`
for SQL execution 0, `e0n9` for its plan node 9, `s4` for stage 4, `x3` for executor 3. A plan
analyzed alone has no execution id, so its node is `n9`, and a finding about the whole query is
`plan`. Analyzing a log again gives the same ids, and a rule giving two findings one id is
refused.

*Rejected:* naming a whole-query finding on a bare plan by its root, `n0`. Its scope is the query,
and a node key would claim it is about one operator.

The id is stable within one log only. Stage numbers shift between runs, so comparing two runs
matches findings by rule and aligned scope, never by this string.

## Each threshold names where it came from

A `Threshold` carries a `source` beside its default. Most defaults come from published prior art:

- sparkDoctor's `docs/detections.md`, which supplies most stage floors, the exchange count and the
  repeated-subtree sizes;
- DataFlint's alert reducers, for wasted cores, memory provisioning and partition size, and for
  join sides, broadcast size, file sizes and filter length;
- Dr. Elephant's `ExecutorGcHeuristic`, for the GC ladder;
- Spark's own defaults, for the partition size at which AQE calls a partition skewed;
- AWS Glue's observability docs, for the skewness score;
- the Databricks Spark UI guide, for a driver gap of a minute and a job of a few seconds.

A threshold no prior art supplies is marked `proposed`. `sparkxray rules show <rule id>` prints
each threshold's source.

## Each applied threshold records the layer that set it

A threshold's value comes from one of four layers, each overriding the one before:

1. the rule's default;
2. a TOML file, with one `["<rule_id>"]` table per rule;
3. the SDK's `thresholds` mapping;
4. a `<rule_id>.<name>=<value>` override string, passed in `threshold_flags`.

Every finding lists each of its rule's thresholds, with the value applied, the layer that set it,
and the default beside it. So a finding judged against an override says so.

A size reads as Spark reads its own byte strings: `512m`, `512mb` and `512MiB` are all 512 × 1024²
bytes. Spark's own default for `skewedPartitionThresholdInBytes` is written `256MB` and means
268,435,456 bytes, so a decimal `MB` would read a copied Spark setting nearly 5% short. A duration takes
`ms`, `s`, `m` or `min`, `h` and `d`.

An override naming a rule or threshold the catalog does not hold is refused, even when the
analysis does not run that rule. So one file serves every subset of rules, and a misspelling never
passes silently.

## Findings are written beside the tables they came from

`analyze` writes `findings.parquet` into the log's table directory, and `tables.findings` reads it.
A finding's scope ids are columns of their own, so a query joins findings to `stages` on
`stage_id`. `evidence` and `thresholds` are JSON text. Reading `findings` before any analysis is
`E-RULE-005`, because an empty view would read the same as a clean log.

The file holds the last analysis only. A finding a threshold silenced leaves no row, and neither
does a rule that never ran. So the file's key-value metadata keeps the analysis document without
its findings, under `sparkxray.analysis`. It names the signals read, every rule run with the
thresholds it applied, and every rule skipped. `tables.stored_analysis()` returns it, and DuckDB's
`parquet_kv_metadata` reads it. It is written in the same file as the rows, so the two never
disagree.

*Rejected:* a sidecar `analysis.json`. Two files are replaced one at a time, and a reader between
the two renames gets one analysis's rules beside another's findings.

## The seed rules measure what the Spark UI shows

`stage.shuffle-skew` reads every task of `tasks_effective` in each stage that read a shuffle, the
tasks that read an empty partition included. The Stages tab's summary metrics and AQE's skew join
both take their median over every task. So where most partitions are empty the median is 0, and
the size floor alone decides, as it does for AQE. The ratio is then null rather than infinite.

It takes the median at sorted index n / 2, as the Stages tab does. DuckDB's `median` interpolates
and its `quantile_disc` takes the lower middle, so either would print a number the UI does not.

Its `aqe_skew_join_enabled` is AQE and its skew join both on. Each comes from the execution's own
`modified_configs`, then the application's Spark properties, then the release's default: AQE is on
from Spark 3.2, and its skew join is on wherever AQE is.

`plan.cartesian` follows DataFlint's cross-join alert. The rows scanned are the product of the two
inputs' rows, and over 10,000,000,000 is `critical`. Each input's rows are the first count down its
chain of single-child nodes. A node's `number of output rows` counts first, and a finished query
stage's measured `rowCount` next. So a plan read from `explain()` text, with no metrics, still
sizes a join whose sides AQE had already run.

## A plan rule sizes what ran, so a plan that never ran stays quiet

A join side or a broadcast takes its bytes from the first measured size down its chain. That is a
finished query stage's `sizeInBytes`, then an exchange's `data size`, then a scan's `size of files
read`. The chain passes through a sort, AQE's reads and a columnar conversion, which keep a side's
size. A filter changes it, so a side measured only below a filter has no size.

`plan.smj-small-side` weighs only a side Spark could broadcast for the join's type, as
`canBuildBroadcastLeft` and `canBuildBroadcastRight` in Spark's optimizer decide. A full outer
join has none. The rule also fires only where the other side passes an `Exchange`, because a
broadcast saves that shuffle. A join of bucketed tables shuffles neither side.

A plan printed before it ran measured nothing. So `plan.smj-small-side` finds no size in it, and
the rules reading metrics are skipped. The optimizer's own estimate is no substitute. With the
cost-based optimizer off, as it is by default, it sizes a join as the product of its inputs, whatever
statistics the tables hold. In the plan-rules fixture, a
396.5 KiB table joined to a 7.1 KiB one is estimated at 2.8 GiB. `plan.estimate-unreliable`
reports that gap, from `explain(mode='cost')` text. A relation the optimizer could not size prints `8.0 EiB`, which is
`Long.MaxValue`, and every estimate above it counts as unknown.

## A cached plan counts once, at the copy that ran

Spark prints a cached relation's plan under every `InMemoryTableScan` that reads it, and every
copy shares the accumulators of the one run that filled the cache. So every plan rule counts each
cached plan once: at its first copy whose metrics show it ran, or at its first copy in a plan with
no metrics. Copies match by their accumulator ids. A plan read from text prints none, so there
copies match by their hash and the locations their scans read. The scan that filled a cache is
then reported once, however many queries read the cache.

## A repeat is confirmed by the files beneath it, and is a miss only once Spark could reuse it

`subtree_hash` writes every path as `<path>`, so two scans of different tables with one schema hash
alike. The repeated-work rules therefore also compare the raw `Location` of every scan beneath each
copy. Of nested repeats, only the widest is reported. A repeat rooted at an exchange belongs to
`plan.missed-exchange-reuse`, and `plan.repeated-subtree` leaves it out.

Spark replaces a repeated exchange with a `ReusedExchange` as it runs the plan. So a plan printed
with `isFinalPlan=false` has reused nothing yet. `plan.missed-exchange-reuse` skips such a plan.
There `plan.repeated-subtree` leaves out every repeat holding an exchange, and `plan.repeated-scan`
every scan under a repeated exchange's later copies.

`plan.missed-exchange-reuse` has high confidence where the execution set `spark.sql.exchange.reuse`
to `false`, because the setting explains the miss. Elsewhere its confidence is low. Spark compares
whole canonical plans, so a difference the normalized hash hides may be what kept it from reusing
the exchange.

## A cut or omitted value reads as unknown, never as empty

Simple text cuts each scan field to 100 characters, ending in `...`, past
`spark.sql.maxMetadataStringLength`. A rule that needs a whole value skips a cut one.
`plan.repeated-scan` still reports a cut location, at low confidence, because two cut paths can
hide two tables.

Formatted text leaves out an empty `PushedFilters` or `PartitionFilters` line. So a formatted scan
that has a `Location` and no such line has an empty list. A scan with no `Location` is not a file
scan, and the line was never there to omit.

## A filter is unpushed only when no pushed filter reads its columns

Spark pushes an `isnotnull` for every column a comparison reads, even when the comparison itself
stays in Spark. So `plan.filter-not-pushed` matches each conjunct of the `Filter` against the pushed
list by the set of columns it reads, in order. An `isnotnull` check, a conjunct on a partition
column, a subquery and a dynamic pruning expression are never counted as unpushed. A conjunct is
reported when no pushed entry over the same columns is left to match it.

`plan.partition-not-pruned` reads the scan's `number of partitions read`. A scan whose
`PartitionFilters` hold only `isnotnull` checks and `dynamicpruningexpression(true)` pruned
nothing. That last form is a dynamic pruning Spark dropped, as it does when no broadcast is left
to reuse. The plan-shapes fixtures' pruning query shows it reading all 5 partitions.

## A Python UDF is reported as `info`, because no JVM metric times it

The Python worker's time shows in no operator metric, only in the stage's run time. So both
Python UDF rules report presence. Their evidence names the stages running the UDF, and those
stages' share of the run's task time. Spark 4 renamed several Python operators, and the rules read
both releases' names.

## A memory finding names where it read the heap's size and its peak

An executor's peak is the most JVM heap any stage or task peak reported for it, as the Executors
tab's Peak JVM Memory OnHeap shows it. Spark writes stage peaks only under
`spark.eventLog.logStageExecutorMetrics`, which is off by default. Without them the peak is a
task-end poll, which can miss the top. So `executor.memory-over` takes medium confidence there,
and `config.event-log-incomplete` names the setting.

The heap's size is the executor's resource profile memory, then `spark.executor.memory`, then
Spark's 1 GiB default. A setting reads as Spark reads it, so a bare number is MiB. The driver's
size comes from `spark.driver.memory` the same way. Each finding names both sources, as
`peak_memory_source` and `executor_memory_source` or `driver_memory_source`.

DataFlint's bars apply to the fullest non-driver executor. Over 95% of its heap is too little, and
under 70% is too much. The suggestion is 20% over the heap that filled, or 20% over the peak.

## Idle cores count the cores each task holds

`executor.idle-cores` follows DataFlint: the share of executor core time, lifetime × cores, that
no task held. A task holds `spark.task.cpus` cores, so two-core tasks keep a two-core executor
busy. In local mode the driver runs the tasks, so the driver is counted instead. A Glue run's
evidence adds the idle DPU-hours, at four cores to a DPU.

## An executor's loss is reported unless it was ended on purpose

`executor.lost` reports every executor removal but those Spark or the cluster manager meant:
dynamic allocation's kill and decommission, and YARN's explicit termination and shutdown. Each
cluster manager words the reason its own way. The exit code is read from YARN's `Exit status: N`,
Kubernetes' `exit code N` and standalone's `exited with code N`.

The loss is `high` where its reason or a task on the executor names running out of memory. That is
an `OutOfMemoryError`, YARN's memory limit, or exit code 52, 137 or 143. Spark exits 52 on an
`OutOfMemoryError`. 137 is SIGKILL, which a container's memory limit sends, and 143 is SIGTERM,
which YARN sends first.
