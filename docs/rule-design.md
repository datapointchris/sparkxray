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
presence only.

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

| Rule family | `impact_seconds` |
| --- | --- |
| skew | the slowest task minus the median task, on the stage's critical path |
| spill | the spilling tasks' run time × (disk spill ÷ (shuffle read + input)), capped at stage wall time |
| tiny tasks, scheduler delay, deserialization | the summed overhead ÷ the stage's concurrent slots |
| driver gaps | the gap lengths |
| retry waste | the failed attempts' run time ÷ concurrent slots |
| plan-only rules | null |

Findings sort by `impact_seconds`, largest first and nulls last, then by severity.

## A rule declares the signals it reads, so it runs wherever they exist

The signals are `plan`, `plan_metrics`, `tasks`, `executors`, `environment` and `timeline`. A rule
whose signals are missing is skipped, and the skip names the missing signal. So `plan.*` rules run
on a bare `explain()` plan with no event log, and a skipped rule says what it would have needed.

Rule ids are dotted by the evidence they read:

| Prefix | Reads |
| --- | --- |
| `plan.` | plan nodes, and their metrics where present |
| `stage.` | effective tasks and stages |
| `executor.` | executors and task memory peaks |
| `app.` | jobs, stages and executors as a timeline |
| `config.` | the environment, against other findings |

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
`_count`, `_share` or `_ratio`. The engine refuses a finding that breaks this, and the findings
schema does too. So a rule registered by other code is held to the same shape. A malformed rule,
a taken id, or a finding that breaks the contract is `E-RULE-004`, naming the rule.

A rule's `title` is a `str.format` template over the finding's scope ids and evidence. In it,
`{key:bytes}` prints a byte count as the Spark UI's `Utils.bytesToString` does, `702.5 MiB`, and
`{key:duration}` prints milliseconds as its `UIUtils.formatDuration` does, `26 s`. So a title reads
the way the Stages tab shows the same number.

## A finding is named by its rule and its scope

`finding_id` is the rule id and a key built from the scope's ids: `app` for the application, `e0`
for SQL execution 0, `e0n9` for its plan node 9, `s4` for stage 4, `x3` for executor 3. A plan
analyzed alone has no execution id, so its node is `n9`. Analyzing a log again gives the same ids,
and a rule giving two findings one id is refused.

The id is stable within one log only. Stage numbers shift between runs, so comparing two runs
matches findings by rule and aligned scope, never by this string.

## Each threshold names where it came from

A `Threshold` carries a `source` beside its default. Most defaults come from published prior art:

- sparkDoctor's `docs/detections.md`, which supplies most stage floors;
- DataFlint's alert reducers, for wasted cores, memory provisioning and partition size;
- Dr. Elephant's Spark heuristics, for the GC ladder and task failure rates;
- AWS Glue's observability docs, for the skewness score;
- kubeflow's Spark tools, for spill and GC;
- Sparklens, for the executor simulation.

A threshold no prior art supplies is marked `proposed`. Scheduler delay, deserialization time and
shuffle fetch wait are the cases.

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
