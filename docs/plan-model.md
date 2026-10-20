# Plan model

## Four plan forms read into one tree

| Source | Where it comes from |
| --- | --- |
| `sparkPlanInfo` JSON | `SparkListenerSQLExecutionStart` and every `SparkListenerSQLAdaptiveExecutionUpdate` in an event log |
| formatted text | `physicalPlanDescription` in the same events, and `df.explain(mode='formatted')` |
| simple text | `df.explain()`, the physical plan section of `explain(mode='extended')` and `explain(mode='cost')`, and `physicalPlanDescription` in AWS Glue's event logs, which Glue writes in extended mode |
| REST SQL graph | a History Server's `/api/v1/applications/{app}/sql/{id}` with details |

`parse_plan_text` tells formatted text from simple text by its first tree line. So a caller never
names the mode.

The model is a tree, not text. A plan from Spark 4 gains a `ResultQueryStage` node directly under
its `AdaptiveSparkPlan`, which Spark 3.5 never writes. Between the two, a line diff marks dozens of
lines, and a node diff marks one node.

## Wrapper nodes are folded, so operator counts match across sources

The JSON tree holds wrappers the text never shows, and the text holds lines JSON never does:

- A `WholeStageCodegen (n)` node is removed. Its `n` becomes `codegen_stage` on each node inside
  it. Its own metric, the pipeline's duration, moves to the topmost node inside it.
- An `InputAdapter` node is removed, and its child is reattached to its parent.
- The `*(n)` prefix in simple text, and `[codegen id : n]` in formatted text's details, also become
  `codegen_stage`.
- Scans canonicalize to `Scan` plus a format. Simple text prints `FileScan parquet`, while
  formatted text and JSON print `Scan parquet`.
- A command loses its `Execute ` prefix. `Execute CreateViewCommand` reads as `CreateViewCommand`.
- Text draws a command's logical plan beneath it. JSON omits it, and so does the model.
- A `ReusedExchange` is a leaf. JSON repeats the reused exchange's whole subtree under it, while
  text prints `[Reuses operator id: n]` and no children.
- A cached relation's plan sits directly under its `InMemoryTableScan`. Text prints an
  `InMemoryRelation` between the two, and it is removed.
- Each subquery becomes its own `Plan` in `Plan.subqueries`, carrying its `subquery_id` and the
  `hosting_node_id` of the node that runs it. JSON holds a subquery as an extra child of that node.
  Simple text draws it beneath the node, and formatted text lists it after the tree, under
  `===== Subqueries =====`.
- The `Subquery` or `SubqueryBroadcast` node wrapping a subquery is removed, as formatted text
  removes it. Its metrics, the result's `data size` and its `time to collect`, move to the
  subquery's root. That root is an `AdaptiveSparkPlan` under AQE, which declares no metrics of its
  own. With AQE off, a `SubqueryBroadcast` sits over the reused `BroadcastExchange`, which declares
  both names too. That node then lists each name twice, told apart by accumulator id.

With those folded, one plan read from JSON and from formatted text gives equal operator counts.
Without them, one plan read two ways reports `FileScan +1, Scan -1`.

`Plan.numbered()` numbers a plan's nodes and its subqueries' in one sequence. The plan's own nodes
keep their `node_id`, and each subquery's follow on from the last number before them. A
subquery's root takes its hosting node as parent. `plan_nodes` uses these numbers, so
`(execution_id, plan_version, node_id)` names any node, and a rule reading a plan names the same
node the tables do.

Labels are normalized by rewriting ids only. Each rewrite is anchored to the syntax that carries
the id, so two builds of one transform compare equal. The ids themselves stay on each node as
fields: expression ids, the plan id, the query stage id and subquery ids.

Names that differ per JVM are rewritten too. A lambda class such as
`$$Lambda$2082/0x0000000840efd040@75e17c26` reads `$$Lambda`. An object's identity hash reads
`@N` only after a class name, as in `NoopWrite$@76babe95`. So a literal like `ops@cafe.example`
survives.

A URI reads `<path>` only after a filesystem scheme and `:/`, and it ends at a space, a comma or a
closing bracket. So `struct<file:string>` survives, and `StartsWith(src#7, s3://raw/)` reads
`StartsWith(src#N, <path>)`.

Spark prints string literals unquoted. A literal holding `#` and digits, such as `PR#42`, reads as
an expression id and normalizes to `PR#N`.

## Text trees are read by column, because inner children sit six columns in

Spark draws a child three columns right of its parent. An inner child sits six columns right: a
cached relation's plan, a command's logical plan, or a subquery in simple text.

`AdaptiveSparkPlan` draws its `== Final Plan ==` and `== Initial Plan ==` headers at a column set
by depth alone. So a block's first node can start in its owner's own column.

The parser therefore never looks for a parent exactly three columns left. A line's parent is the
nearest open node to its left. A block belongs to the nearest open `AdaptiveSparkPlan` at or left
of its header.

## Only the last AQE update of an execution is the final plan

`SQLExecutionStart` carries the initial plan. Adaptive query execution re-plans as stages finish,
and each re-plan emits a `SQLAdaptiveExecutionUpdate` with a new tree and text.

- An intermediate update prints `== Current Plan ==` under `AdaptiveSparkPlan`.
- The last update prints `== Final Plan ==` and then `== Initial Plan ==`. Its JSON root holds
  `AdaptiveSparkPlan isFinalPlan=true`.
- With AQE off there are no updates, and the initial plan is final.

The JSON child of `AdaptiveSparkPlan` is the current plan only. The initial plan survives in the
text alone, and the model exposes it through `Plan.initial()`.

A cached relation's plan is one object, which both blocks print. So the initial block of a final
update shows that plan finished, with its query stages, even though it had not run when the
execution started.

Updates carry no timestamp. Their order is their position in the log, and the tables keep that
order explicitly.

`last_plans` reads each execution's last plan event. An execution that never finished keeps the
last plan AQE reached, with `is_final` False. Killed and failed runs are the ones most worth
explaining, so they are never dropped.

It takes the tree from `sparkPlanInfo`, because only JSON carries accumulator ids. Where the text
reads into the same operators in the same shape, each node also takes the text's formatted number,
details and statistics. The plan then takes the text's initial plan. Where the two differ, the plan
is JSON alone.

Formatted text numbers each plan object once, so a node AQE left alone carries one number in both
blocks. Each such initial node takes its final node's label, details and metrics. So an untouched
node has one `normalized_label` and `subtree_hash` in both plans, and an initial-to-final diff
marks only what AQE changed.

Finished query stages print their measured size, as in `Statistics(sizeInBytes=8.4 MiB, …)`. These
come from completed shuffles, not cost-model guesses, so plan rules can size a join's sides with no
task metrics. Spark 4 prints `8.0 EiB` on `ResultQueryStage`. That is `Long.MaxValue` used as a
placeholder, and the parser reads it as unknown.

## What AQE changed is the first plan against the last, less what every re-plan adds

`aqe_changes` compares the plan `SQLExecutionStart` carries, version 0, with the last version.
Three parts make up the result.

- **Operator counts that changed.** AQE adds a query stage around each stage it creates, and an
  `AQEShuffleRead` over each shuffle. Its `postStageCreationRules` insert `ColumnarToRow` and
  `RowToColumnar` as each stage is created. So the first plan never holds any of them, and the
  counts leave them out.
- **Every `AQEShuffleRead` of the last plan.** Each comes with the kind Spark printed beside it:
  `coalesced`, `skewed`, `coalesced and skewed` or `local`. Each also carries its partition
  metrics, which the driver sets once, and the partition count of the `Exchange` it reads. A
  `ReusedExchange` prints the exchange it reads again after its output, so the count is read from
  there.
- **Every join AQE switched or split for skew.** A join keeps its keys across one execution's plans,
  because expression ids do. So each join of the last plan is paired, in pre-order, with the first
  plan's join on the same keys and type. A pair is reported where the operator changed, or where the
  last plan prints `(skew=true)` after it.

Reuse shows in the counts as nodes the last plan lacks. AQE reuses a subquery as it optimizes each
stage, so the first plan holds one copy per node that uses it. A shuffle stage AQE reuses prints as
a `ReusedExchange` in place of its second copy.

A join on a condition alone prints no keys, so it is never paired. A switch between a Cartesian
product and a nested-loop join shows only in the operator counts.

## Cost-mode text adds the optimizer's estimates, kept beside the tree

`explain(mode='cost')` prints the optimized logical plan, each node ending in
`Statistics(sizeInBytes=…)`, then the physical plan. `parse_plan_text` reads the physical plan as
the tree, as for any text. It reads the logical plan into `Plan.estimates`, one `LogicalEstimate`
per node in the order printed, each naming its parent and children. Every other form leaves
`estimates` empty.

An estimate is the optimizer's guess before anything ran, where a finished query stage's size was
measured. A relation without table statistics is sized from its files. One the optimizer could not
size prints `8.0 EiB`. That relation and every node above it have `size_bytes` None, because each
of their estimates grew from the placeholder.

The logical and physical trees share no node ids. One logical join can become an exchange, a sort
and a join. So the estimates are a list beside the plan, never a field on its nodes.

## A truncated plan is flagged, never read as a smaller one

Spark truncates plan text three ways:

- `... 16 more fields]` inside a node, past `spark.sql.debug.maxToStringFields`;
- `... 2914 more characters` at the cut, past `spark.sql.maxPlanStringLength`;
- the whole text replaced by `Truncated plan of 2260 characters`, when that length is 0.

Each sets `Plan.truncated`. A cut text reads as its tree up to the cut. A text cut before its
physical plan section reads as an empty plan. `maxPlanStringLength` never cuts the JSON, so
`last_plans` still reads such an execution's whole tree, and flags it.

## The History Server's graph has no query stages

The REST graph is a list of nodes and edges, not a tree. `WholeStageCodegen (n)` clusters appear
as nodes, and fold as JSON's do. The graph holds no query-stage node and no `InputAdapter`, and
Spark 4's has no `ResultQueryStage`. A reused exchange is a second edge into the original exchange,
and reads as a `ReusedExchange` leaf.

So a REST plan's operator counts equal the JSON plan's with its query stages removed. A REST plan
carries no version, because the API serves only the execution's latest plan.

Its metrics are display strings, not accumulator updates:

```text
1,024
9.9 KiB
12 ms (6 ms, 6 ms, 6 ms (stage 28.0: task 37))
```

`parse_display_value` reads each into a unit and a total. Where Spark prints the spread, it also
reads the minimum, median and maximum, and the stage and task that held the maximum.

## Plan nodes reach the stages that ran them through accumulator ids

Each node in `sparkPlanInfo` lists its metrics with an accumulator id. Tasks report updates to
those ids, and each stage lists the ids it touched. Joining the two maps nodes to stages.

AQE allocates fresh accumulators every time it re-plans a node, and even the first update
re-allocates every node. A query stage that finished before a re-plan then keeps its node and ids.
So the last plan lists the ids of everything that actually ran.

That is why rules read metrics through the last plan's nodes. A node of the version 0 plan
`SQLExecutionStart` logs has no updates, which is expected, not missing data.

Driver-side values, such as broadcast build time and file listing, arrive in
`SparkListenerDriverAccumUpdates`. Metric definitions for a subquery AQE re-planned arrive in
`SparkListenerSQLAdaptiveSQLMetricUpdates`, naming no node. Both join the same way, and a
definition with no node has no plan version or node id in `plan_node_metrics`.

## Plan metrics aggregate as the SQL tab aggregates them, except where it names a task

`plan_node_metrics` follows Spark's `SQLAppStatusListener` and `MetricUtils.stringValue`, which
build the History Server's SQL tab:

- Only each stage's highest attempt counts. The listener starts a stage's values afresh when a new
  attempt is submitted.
- A task index keeps its last reported value up to its first success. A retry replaces a failed
  attempt's value, and a speculative copy finishing after the success changes nothing.
- An index that reported nothing counts as 0. The listener sizes each stage's values by its task
  count, and a retried attempt that reran a few tasks leaves the rest at 0.
- The driver's values count beside the tasks'.
- A `sum` counts every value, and an `average` those above 0. A `size`, `timing` or `nsTiming`
  counts those at 0 or above, which drops the -1 an unset metric starts at.
- Every type gets `total`, `min`, `median` and `max`, and the stage and task that held the max.
  The median is the value at index n / 2 of the sorted values, as Spark takes it. `value_count` is
  how many values those cover.
- A `size` is in bytes, a `timing` in milliseconds and a `sum` a count. An `nsTiming` is converted
  to milliseconds, truncated. An `average` has no unit and no total. Spark stores it as ten times
  its value, so a long integer keeps one decimal, and the table divides by 10.

The tables differ from the SQL tab in two places:

- Spark's listener names the first task to report the highest value it has seen, a failed attempt
  included, and keeps that name after a retry replaces the value. So the SQL tab can print a
  maximum beside a task that never reported it. The tables name the task whose value is the
  maximum.
- A metric with no counted value has null statistics, where the SQL tab prints zero.

A driver value above every task's names no task, so `max_stage_id` and `max_task_id` are null
where the SQL tab prints `(driver)`.

`fixtures/history-server-sql/` holds what the SQL tab serves for the plan-shapes logs, metric
strings included. The tables test prints each execution's `plan_node_metrics` the way the SQL tab
prints them, and requires the two to match.
