"""One metric in both runs: each run's value, the change, and whether the change is big enough to read first."""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from sparkxray.tables.registry import column_unit

# The metrics a stage pair compares. An execution pair compares all but the two ratios, because a
# ratio of one task to the median means nothing across the stages of an execution.
STAGE_METRICS = (
    'wall_ms',
    'executor_run_time_ms',
    'executor_cpu_time_ns',
    'task_count',
    'input_bytes',
    'input_records',
    'output_bytes',
    'output_records',
    'shuffle_read_bytes',
    'shuffle_read_records',
    'shuffle_write_bytes',
    'shuffle_write_records',
    'memory_spilled_bytes',
    'disk_spilled_bytes',
    'gc_time_ms',
    'task_duration_max_to_median_ratio',
    'shuffle_read_max_to_median_ratio',
)
EXECUTION_METRICS = STAGE_METRICS[:-2]
# A change is notable when it moves by this share of run A's value and by the floor its unit sets.
# A share alone is noise on small numbers: 40 ms to 80 ms doubles and costs nothing. A count's floor
# is the share itself. Both are proposed, as no prior tool publishes one.
NOTABLE_SHARE = 0.1
NOTABLE_FLOORS: Mapping[str, float] = {'ms': 5_000, 'ns': 5_000_000_000, 'bytes': 64 * 2**20, 'x': 1}


@dc.dataclass(frozen=True, slots=True)
class MetricDelta:
    """One metric in run A and run B, in `unit`. `delta_share` is the change over A's value, and None where A's is 0 or missing."""

    metric: str
    unit: str
    a: float | None
    b: float | None
    delta: float | None
    delta_share: float | None
    notable: bool

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


def metric_deltas(a: Mapping[str, Any], b: Mapping[str, Any], metrics: Sequence[str]) -> tuple[MetricDelta, ...]:
    """Each metric of `metrics` in the two runs' rows, in that order."""
    return tuple(_delta(metric, a.get(metric), b.get(metric)) for metric in metrics)


def summed(rows: Iterable[Mapping[str, Any]], metrics: Sequence[str]) -> dict[str, Any]:
    """Each metric summed over the rows, None where no row holds it."""
    totals: dict[str, Any] = dict.fromkeys(metrics)
    for row in rows:
        for metric in metrics:
            if row.get(metric) is not None:
                totals[metric] = (totals[metric] or 0) + row[metric]
    return totals


def _delta(metric: str, a: float | None, b: float | None) -> MetricDelta:
    unit = column_unit(metric) or ''
    if a is None or b is None:
        return MetricDelta(metric, unit, a, b, None, None, notable=False)
    delta = b - a
    share = round(delta / a, 4) if a else None
    floor = NOTABLE_FLOORS.get(unit, NOTABLE_SHARE * abs(a))
    moved_share = abs(delta) >= NOTABLE_SHARE * abs(a)
    return MetricDelta(metric, unit, a, b, round(delta, 4), share, notable=delta != 0 and moved_share and abs(delta) >= floor)
