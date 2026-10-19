# Plan model

## Four plan forms read into one tree

| Source | Where it comes from |
| --- | --- |
| `sparkPlanInfo` JSON | `SparkListenerSQLExecutionStart` and every `SparkListenerSQLAdaptiveExecutionUpdate` in an event log |
| formatted text | `physicalPlanDescription` in the same events, and `df.explain(mode='formatted')` |
| simple text | `df.explain()`, the physical plan section of `explain(mode='extended')`, and `physicalPlanDescription` in AWS Glue's event logs, which Glue writes in extended mode |
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

With those folded, one plan read from JSON and from formatted text gives equal operator counts.
Without them, one plan read two ways reports `FileScan +1, Scan -1`.

Labels are normalized by rewriting ids only. Each rewrite is anchored to the syntax that carries
the id, so two builds of one transform compare equal. The ids themselves stay on each node as
fields: expression ids, the plan id, the query stage id and subquery ids.

Names that differ per JVM are rewritten too. A lambda class such as
`$$Lambda$2082/0x0000000840efd040@75e17c26` reads `$$Lambda`. An object's identity hash, as in
`NoopWrite$@76babe95`, reads `NoopWrite$@N`.

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

`final_plans` reads each execution's last plan event, and leaves out one whose last plan is not
final, as when the application was killed mid-query. It takes the tree from `sparkPlanInfo`,
because only JSON carries accumulator ids. Where the text reads into the same operators in the same
shape, each node also takes the text's formatted number, details and statistics. The plan then
takes the text's initial plan. Where the two differ, the plan is JSON alone.

Finished query stages print their measured size, as in `Statistics(sizeInBytes=8.4 MiB, …)`. These
come from completed shuffles, not cost-model guesses, so plan rules can size a join's sides with no
task metrics. Spark 4 prints `8.0 EiB` on `ResultQueryStage`. That is `Long.MaxValue` used as a
placeholder, and the parser reads it as unknown.

## A truncated plan is flagged, never read as a smaller one

Spark truncates plan text three ways:

- `... 16 more fields]` inside a node, past `spark.sql.debug.maxToStringFields`;
- `... 2914 more characters` at the cut, past `spark.sql.maxPlanStringLength`;
- the whole text replaced by `Truncated plan of 2260 characters`, when that length is 0.

Each sets `Plan.truncated`. A cut text reads as its tree up to the cut. A text cut before its
physical plan section reads as an empty plan. `maxPlanStringLength` never cuts the JSON, so
`final_plans` still reads such an execution's whole tree, and flags it.

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
So the final plan lists the ids of everything that actually ran.

That is why rules read metrics through the final plan's nodes. An initial-plan node with no
updates is expected, not missing data.

Metrics aggregate per node the way the History Server's SQL tab shows them:

| Metric type | Aggregated as |
| --- | --- |
| `sum`, `size` | total, plus min, median and max across tasks, and which stage and task held the max |
| `timing` | milliseconds |
| `nsTiming` | nanoseconds, reported in milliseconds |
| `average` | Spark's per-task average |

Driver-side values, such as broadcast build time and file listing, arrive in
`SparkListenerDriverAccumUpdates`. Metric definitions for nodes AQE added arrive in
`SparkListenerSQLAdaptiveSQLMetricUpdates`. Both join the same way.
