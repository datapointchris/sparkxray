# Rule design

## A ratio that can gate a run is paired with an absolute floor

A ratio alone fires on noise. In the `spark-3.5.9` fixture, the 200-file write's stage runs 200
tasks with a 14 ms median and a 385 ms maximum. That is a 27× max-to-median duration ratio, and
nothing there is worth fixing.

Durations also understate real skew. The same fixture's skewed join reads 64× the median's shuffle
records on its largest task, but runs only 38× the median's time. `fixtures/README.md` gives both
stages' totals and the command that reads them.

So every ratio that can gate a run comes with an absolute amount, the way AQE's own skew test
pairs `skewedPartitionFactor` (5) with `skewedPartitionThresholdInBytes` (`256MB`, which Spark
reads as 256 MiB). `plan.estimate-unreliable` judges a ratio alone. It reports only at `info`, so
it never gates.

Stage rules lead with bytes and records, and use duration as a secondary signal.
`stage.task-skew` needs a gap of at least 30 s between the slowest and the median task, which
keeps it silent on the 200-task stage.
`stage.scheduler-delay` and `stage.deserialization` need 30 s at stake too: their summed time over
the stage's concurrent slots. So the first stage on fresh executors, which loads its classes
once, stays quiet.

## Severity is the rule's grade, moved one step by its scope's share of the run

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
An exit 3 with no `gate` on stdout means the log was not read. `Analysis.gate` refuses a severity
outside the ladder as `E-RULE-006`, listing the five. The command's parser refuses one first, as
`E-USAGE-001`.

## `impact_seconds` is wall time a fix could recover, and null where nobody can say

A rule prices its own finding, in its SQL or its `check`. A stage rule divides summed task time by
the stage's concurrent slots, so it states wall time rather than core time. A stage's concurrent
slots are its task count or the cores of the executors that ran its tasks, whichever is fewer. A
skew rule takes the slowest task's time less the median's, because the stage waits on that task
alone. `stage.spill` scales the spilling tasks' time by the disk spill's share of what the stage
read, and caps it at the stage's wall time. `app.driver-gaps` sums its gaps. A rule that cannot
say what a fix recovers returns null, as every `plan.*` rule does.
`rg -n impact_seconds library/src/sparkxray/rules` shows each rule's formula.

Findings sort by `impact_seconds`, largest first and nulls last, then by severity.

## A rule declares the signals it reads, so it runs wherever they exist

A signal names one kind of input a rule reads. A plan gives its tree, its metrics and its
estimates, and the stages that ran each node, which a log rebuilt from REST JSON lacks. A log
gives its tasks, executors, environment and timeline. `whole_run` is a run the server held whole,
which one past its `spark.ui.retained*` limits does not. Each table `enrich` joins is a signal of
its own. `sparkxray capabilities show --json` lists every signal under `signals`. `SIGNALS` in
`rules/rule.py` declares them, and `_SIGNALS_SQL` in `rules/analysis.py` decides whether a log
holds each. So a new signal adds a column there.

A rule whose signals are missing is skipped, and the skip names the missing signal. So a `plan.*`
rule reading the plan alone runs on a bare `explain()` plan with no event log. One reading
`plan_metrics` is skipped there, and says it needed them.

`plan_estimates` is the optimizer's size estimates. Only `explain(mode='cost')` text prints them,
and no event log carries them. So `plan.estimate-unreliable` runs through `analyze_plan` alone.
`analyze` leaves it out of a log's default selection, so a log's analysis neither runs it nor
lists it as skipped. Named with `rules=`, it is skipped, and `findings list` says it runs on
cost-mode text only.

Rule ids are dotted by the evidence they read:

| Prefix | Reads |
| --- | --- |
| `plan.` | plan nodes, and their metrics where present |
| `stage.` | one stage's tasks, its effective ones or every attempt, and its own events |
| `executor.` | executors and the driver: cores against task time, memory peaks against settings, and removals |
| `app.` | the job timeline, or the driver's log and Glue's metrics |
| `config.` | the settings, against what the run shows: its tasks, its executors or another rule's findings |

## A rule may read another rule's findings, and that rule runs first

A Python rule names the rules whose findings it reads in `reads_findings_of`. Its context's
`findings` holds them by rule id. Selecting the rule runs those it reads first. Where one of
theirs lacks a signal, the reading rule skips too.

A rule run only because a selected rule reads it reports nothing. Its findings reach the reading
rule alone, and `rules_run` lists it with `reported` false. So `findings list --rule
stage.oversized-partitions --fail-on high` lists and gates that rule's findings, and never the
`stage.shuffle-skew` finding it read. A narrowing flag that gated on rules the caller never named
would fail a CI job for a reason it did not ask about.

Only a Python rule may read findings, and no rule reads its own. `register` refuses a rule reading
one the catalog does not hold. So the order is always known before the first rule runs.

`rules list --json` names the rules each rule reads. A rule reading `stage.shuffle-skew` either
leaves out the stages it found, as `stage.oversized-partitions` and `stage.spill-skew` do, or
builds on them, as `config.skew-join-off-with-skew` does. A skewed partition is too large, and
spills unevenly, because it is skewed, and the skew finding names the fix. A rule that would point
a run the wrong way stays silent where another finding proves the opposite, as
`executor.memory-over` does where a task or an executor ran out of memory.

*Rejected:* reading the findings table from SQL. It holds the last analysis, which need not have
run the rule read, or run it with the same thresholds.

## A SQL rule reads its thresholds as parameters, and a Python rule reads the plans

A SQL rule runs in DuckDB over the tables, and reads each threshold as a `$name` parameter. DuckDB
refuses a parameter a query never reads, so a `Rule` whose SQL does not name each of its thresholds
is refused when it is built. A Python rule's `check` receives each execution's last plan, numbered
as `plan_nodes` numbers it, with each node's metric totals and the tables.

Either returns one mapping per finding, and the engine sorts its keys three ways:

- `severity`, `impact_seconds`, `confidence` and `confidence_reason` fill the finding's own
  fields. `confidence` is `high`, `medium` or `low`, and `high` where the rule gives none.
  `confidence_reason` says what lowered it;
- the scope ids fill `scope`, and a plan node's label is read from the plan where a rule gives none;
- every other key is evidence.

A numeric evidence key must end in its unit, such as `_bytes`, `_ms`, `_count` or `_share`.
`UNIT_SUFFIXES` in `rules/findings.py` holds every one. A single id ends in `_id`, and a list of
node or stage ids in `_ids`. The engine refuses a finding that breaks this, and the findings
schema does too. So a rule registered by other code is held to the same shape. A malformed rule,
a taken id, or a finding that breaks the contract is `E-RULE-004`, naming the rule.

A rule's `title` is a `str.format` template over the finding's scope ids and evidence. In it,
`{key:bytes}` prints a byte count as the Spark UI's `Utils.bytesToString` does, `702.5 MiB`, and
`{key:duration}` prints milliseconds as its `UIUtils.formatDuration` does, `26 s`. So a title reads
the way the Stages tab shows the same number. `{key:plural gap}` prints a count with its noun
agreeing, `1 gap` or `3 gaps`, so a title never reads "1 gaps". A noun with an irregular plural
names both, as `{key:plural copy|copies}`. A count the defaults hold at 2 or more, such as a
repeated subtree's occurrences, prints bare.

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

A rule id never changes meaning, so renaming a rule breaks every caller. `rule_version` starts at
1 and rises whenever what the rule reports changes. So a finding from an older analysis reads as
judged by an older rule.

## A stage finding names the plan node behind it where one stands out

A stage runs several operators of one execution's plan. `stage.shuffle-skew` names the join that
read the skewed shuffle, where exactly one sort-merge or shuffled hash join ran in the stage.
`stage.spill` and `stage.spill-skew` name the node that spilled most in the execution's last
plan, whatever its operator: a sort, an aggregate, a window or a join. A spill no plan node counts
names none. A shuffle write's
sorter spills that way.

A node is ranked by its own part of `spill size`. `SortExec` records all the task spilled while
it read its input, so a sort's figure also counts the spills of every node below it in the task.
`plan_nodes.enclosing_sort_id` names that sort for each node, up to the first exchange or query
stage. The shared `own_spills` SQL takes those nodes' figures off the sort's. Ranked by the raw
figure, the outermost sort of a task would always win.

In stage 4 of the `spark-3.5.9` fixture, the sort above the join records 6.1 GiB, the stage's
whole spill. Its own part is 4.1 GiB. The two sorts under the join hold the rest, 1562.0 MiB and
560.0 MiB. `sparkxray plans show <log> --sql 0` prints all three.

The node joins the finding's scope, and its id stays the stage's. The finding is about the stage,
and the node says where in the plan to look.

## Each threshold names where it came from

A `Threshold` carries a `source` beside its default. Most defaults come from published prior art:
SparkDoctor's `docs/detections.md`, DataFlint's alert reducers, Dr. Elephant's
`ExecutorGcHeuristic`, Spark's own defaults, AWS Glue's observability docs and the Databricks Spark
UI guide. Each `source` names the file or page. A threshold no prior art supplies is marked
`proposed`. `sparkxray rules show <rule id>` prints each threshold's source, and
`sparkxray capabilities show --json` lists every rule's.

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
268,435,456 bytes, so a decimal `MB` would read a copied Spark setting nearly 5% short. A
duration takes `ms`, `s`, `m` or `min`, `h` and `d`.

An override naming a rule or threshold the catalog does not hold is refused, even when the
analysis does not run that rule. So one file serves every subset of rules, and a misspelling never
passes silently.

## A lesson names a threshold as `{name}`, so it prints at the value the rule applies

Every rule's lesson answers four questions, under four headings: what it measures, why it costs
time, where the Spark UI shows it, and how to fix it. A lesson that states a threshold names it as
`{min_bytes}`, never as a number. `Rule.lesson_text(values)` prints each at the value given, or at
its default, as a sentence would: `256 MiB`, `5 min`, `8%`, `5x` or `100,000,000`. So
`sparkxray rules show stage.task-skew --threshold stage.task-skew.min_gap_ms=1000` says "at least
1 s past the median". Many lessons state no threshold in their prose. Every `rules show` lists
each threshold at its applied value in its Thresholds table, with the layer that set it.

A placeholder must name one of the rule's thresholds, and takes no format or conversion. A brace
that opens no placeholder is refused too. Each fails when the rule is built, as `E-RULE-004`.

*Rejected:* writing the number into the prose. An override would leave the lesson stating a floor
the rule no longer judges by.

*Rejected:* `$name`. A SQL rule already reads its thresholds as `$name` parameters.

## A lesson ends with what the rule found on a fixture log

`sparkxray rules show` follows the lesson with one finding the rule reported on a committed
fixture: its title, scope, severity and evidence, and the log it came from. The library ships them
in `sparkxray/rules/lesson_examples.json`, and `rule_lesson(rule_id)` returns one beside the
lesson. A rule no fixture fires has none, as `app.driver-memory-pressure`, which needs Glue's
CloudWatch metrics. `fixtures/write_lesson_examples.py` writes the file. The rules test fails on an
example its log no longer reports, so a lesson never shows output the rule has stopped giving.

*Rejected:* evidence written by hand. Nothing would hold it to what the rule reports.

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

## `stage.shuffle-skew` takes its median as the Stages tab does, and `plan.cartesian` sizes a cross join as DataFlint does

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
stage's measured `rowCount` next. So a plan read from `explain(mode='formatted')` text, with no
metrics, still sizes a join whose sides AQE had already run. Simple text prints no measured size.

## A plan rule sizes what ran, so a plan that never ran stays quiet

A join side takes its bytes from the first measured size down its chain. That is a finished
shuffle query stage's `sizeInBytes`, then an exchange's `data size`, then a scan's `size of files
read`. The chain passes through a sort, a coalesce, AQE's reads and a columnar conversion, which
keep a side's size. A filter changes it, so a side measured only below a filter has no size.
`plan.broadcast-too-large` reads the broadcast exchange's own `data size`, so it needs the plan's
metrics.

`plan.smj-small-side` weighs only a side Spark could broadcast for the join's type, as
`canBuildBroadcastLeft` and `canBuildBroadcastRight` in Spark's optimizer decide. A full outer
join has none. The rule also fires only where the other side passes an `Exchange`, because a
broadcast saves that shuffle. A join of bucketed tables shuffles neither side.

A plan printed before it ran measured nothing. So `plan.smj-small-side` finds no size in it, and
the rules reading metrics are skipped. The optimizer's own estimate is no substitute. With the
cost-based optimizer off, as it is by default, it sizes a join as the product of its inputs,
whatever statistics the tables hold. In `fixtures/explain/spark-3.5.9-plan-rules/cost-mode.json`, a
396.5 KiB table joined to a 7.1 KiB one is estimated at 2.8 GiB. `plan.estimate-unreliable`
reports that gap, from `explain(mode='cost')` text. A relation the optimizer could not size prints
`8.0 EiB`, which is `Long.MaxValue`, and every estimate above it counts as unknown.

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

`plan.missed-exchange-reuse` has high confidence where the execution ran with
`spark.sql.exchange.reuse` set to `false`, by its own settings or the application's. The setting
explains the miss. Elsewhere its confidence is low. Spark compares
whole canonical plans, so a difference the normalized hash hides may be what kept it from reusing
the exchange.

## A cut value reads as unknown, and a list formatted text omits reads as empty

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
stages' share of every effective task's run time, as `stage_run_time_share`. Spark 4 renamed
several Python operators, and the rules read both releases' names.

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

An allocation that fails never reaches the peak, and a lost executor's last metrics can go with
it. So a run where a task ran out of memory can show every peak low. `executor.memory-over` stays
silent where `stage.task-oom` fired or `executor.lost` graded a loss high, rather than advise
cutting the memory that ran out.

## Idle cores count the cores each task holds

`executor.idle-cores` follows DataFlint: the share of executor core time, lifetime × cores, that
no task held. A task holds `spark.task.cpus` cores, so two-core tasks keep a two-core executor
busy. In local mode the driver runs the tasks, so the driver is counted instead. A Glue run's
evidence adds the idle DPU-hours, at four cores to a DPU. DataFlint pairs its 50% share with no
amount, so a run of seconds fires on its start-up alone. The rule also needs `min_idle_core_ms`,
five minutes of one core by default, before the share counts. The `spark-3.5.9` fixture, a
50-second local run on four cores, leaves 95 core-seconds idle, as `sparkxray executors list`
reads its lifetime, cores and task time. So the floor sits about three times above that.

## Glue's figures sit beside a log rule's evidence, and decide only the rules built on them

Where a Glue run is joined, two rules add Glue's figure beside the one the log gives:

- `stage.task-skew` adds `glue_metrics_skewness_ratio`, the highest `glue.driver.skewness.stage`
  over any period the stage ran in.
- `executor.idle-cores` adds `glue_worker_utilization_share`, the run's mean
  `glue.driver.workerUtilization`.

Neither figure changes whether its rule fires. Glue's stage skewness carries no stage dimension.
So stages that ran at once share one value, while the log's ratio comes from the stage's own
tasks.

`app.driver-memory-pressure` reads Glue's heap metric alone, and fires on a span, never a peak.
The heap fills between collections, so one full sample can be a collection about to run. It
reports each run of consecutive periods at or over `min_heap_used_fraction` that lasted at least
`min_sustained_ms`. Glue publishes every 30 seconds, but sparkxray reads one-minute averages for a
run under 15 days old, the shortest period it asks CloudWatch for. So the default is two of those
minutes. An older run reads 5-minute or hourly periods. There one period outlasts
`min_sustained_ms`, so a single period at or over the floor fires. No prior art gives either
threshold, so both are proposed.

`app.driver-oom` reads the driver's log alone, because the event log holds no trace of a driver
OOM. In the fixture run, a `collect` larger than the driver's heap ended its job as succeeded. Its
log closed with the application's end event, as a clean run's does. A record carrying
`java.lang.OutOfMemoryError` is the only sign, so one is enough to fire, at high.

## An executor's loss is reported unless it was ended on purpose

`executor.lost` reports every executor removal but those Spark or the cluster manager meant:
dynamic allocation's kill and decommission, and YARN's explicit termination and shutdown. Each
cluster manager words the reason its own way. The exit code is read from YARN's `Exit status: N`,
Kubernetes' `exit code N` and standalone's `exited with code N`.

The loss is `high` where its reason or a task on the executor names running out of memory. That is
an `OutOfMemoryError`, YARN's memory limit, Kubernetes' `OOMKilled`, or exit code 52, 137 or
143. Spark exits 52 on an `OutOfMemoryError`. 137 is SIGKILL, which a container's memory limit
sends, and 143 is SIGTERM, which YARN sends first.

## A stage or job Spark stopped for another's failure is not a failure

When a job fails or is cancelled, Spark's `DAGScheduler` ends every running stage only that job
ran, and gives each the job's reason. A failed job's reason blames the stage whose task failed, as
`Job aborted due to stage failure: Task 0 in stage 2.0 failed 4 times`. A cancelled job's reads
`Job 4 cancelled …`, and Spark 4 prefixes it with `[SPARK_JOB_CANCELLED]`. AQE cancels the other
query stages of a query when one fails, and each cancellation ends a job of its own.

So `stage.failed` reports a stage only where its reason blames that stage. Spark 4 passes a
user-facing error through unwrapped, as `[PYTHON_EXCEPTION] …`, which blames no stage. There a
stage counts where one of its own task attempts failed rather than being killed, because a
stopped sibling's running tasks end killed. `app.failed-jobs` counts cancelled jobs apart, in
`cancelled_job_count`, and reads its reason from the first job that was not cancelled.

A stage that fails before any task runs, such as on task serialization, blames no stage and has
no failed attempt, so `stage.failed` leaves it out. `app.failed-jobs` still reports its job.
