# Plan model

## Four plan forms read into one tree

| Source | Where it comes from |
| --- | --- |
| `sparkPlanInfo` JSON | `SparkListenerSQLExecutionStart` and every `SparkListenerSQLAdaptiveExecutionUpdate` in an event log |
| formatted text | `physicalPlanDescription` in the same events, and `df.explain(mode='formatted')` |
| simple text | `df.explain()`, and the physical plan section of `explain(mode='extended')` |
| REST SQL graph | a History Server's `/api/v1/applications/{app}/sql/{id}` with details |

The model is a tree, not text. A plan from Spark 4 gains a `ResultQueryStage` node at its top that
Spark 3.5 never writes. Between the two, a line diff marks dozens of lines, and a node diff marks
one node.

## Wrapper nodes are folded, so operator counts match across sources

The JSON tree holds wrappers the text never shows:

- A `WholeStageCodegen (n)` node is removed. Its `n` becomes `codegen_stage` on each node inside
  it.
- An `InputAdapter` node is removed, and its child is reattached to its parent.
- The `*(n)` prefix in simple text and the `*` prefix in formatted text also become
  `codegen_stage`.
- Scans canonicalize to `Scan` plus a format. Simple text prints `FileScan parquet`, while
  formatted text and JSON print `Scan parquet`.

With those folded, one plan read from JSON and from formatted text gives equal operator counts.
Without them, one plan read two ways reports `FileScan +1, Scan -1`.

Labels are normalized by rewriting ids only. Each rewrite is anchored to the syntax that carries
the id, so two builds of one transform compare equal. The ids themselves stay on each node as
fields: expression ids, the plan id, the query stage id and subquery ids.

## Only the last AQE update of an execution is the final plan

`SQLExecutionStart` carries the initial plan. Adaptive query execution re-plans as stages finish,
and each re-plan emits a `SQLAdaptiveExecutionUpdate` with a new tree and text.

- An intermediate update prints `== Current Plan ==` under `AdaptiveSparkPlan`.
- The last update prints `== Final Plan ==` and then `== Initial Plan ==`. Its JSON root holds
  `AdaptiveSparkPlan isFinalPlan=true`.
- With AQE off there are no updates, and the initial plan is final.

The JSON child of `AdaptiveSparkPlan` is the current plan only. The initial plan survives in the
text alone, and the model exposes it separately.

Updates carry no timestamp. Their order is their position in the log, and the tables keep that
order explicitly.

Finished query stages print their measured size, as in `Statistics(sizeInBytes=8.4 MiB, …)`. These
come from completed shuffles, not cost-model guesses, so plan rules can size a join's sides with no
task metrics. Spark 4 prints `8.0 EiB` on `ResultQueryStage`. That is `Long.MaxValue` used as a
placeholder, and the parser reads it as unknown.

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
