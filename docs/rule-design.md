# Rule design

## Every ratio threshold is paired with an absolute floor

A ratio alone fires on noise. A stage of 200 tiny tasks, with a 24 ms median and a 486 ms maximum,
scored a 20.3× max-to-median duration ratio. Nothing there is worth fixing.

Durations also understate real skew. A skewed join stage showed 8.9× in shuffle records but only
2.4× in task duration.

So every ratio comes with an absolute amount, the way AQE's own skew test pairs
`skewedPartitionFactor` (5) with `skewedPartitionThresholdInBytes` (256 MB). Stage rules lead with
bytes and records, and use duration as a secondary signal. `stage.task-skew` needs a gap of at
least 30 s between the slowest and the median task, which keeps it silent on the 200-task stage.

## Severity is the rule's grade, moved one step by the stage's share of the run

The ladder is `info`, `low`, `medium`, `high`, `critical`. `info` is for rules that report
presence only.

Each rule's own ladder sets the base grade. The engine then lowers it one step when the scope
takes under 5% of application wall time, and raises it one step above 50%. So a skewed 2-second
stage does not outrank a balanced 2-hour one. The base grade and the share stay in the evidence,
so the adjustment is visible.

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
