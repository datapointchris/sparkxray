"""The executor simulation's replay gives the diamond DAG's times and every time Sparklens's own scheduler suite expects."""

import pytest
from sparkxray.simulation import JobSpan
from sparkxray.simulation import RunModel
from sparkxray.simulation import StageWork
from sparkxray.simulation import group_jobs

# 0 -> {1, 2} -> 3, where stage 3 also reads stage 9, which the log lacks because it was skipped.
_DIAMOND = {0: StageWork((1, 1)), 1: StageWork((3, 3), (0,)), 2: StageWork((5, 3), (0,)), 3: StageWork((5, 3), (1, 2, 9))}


def _serial(count: int) -> dict[int, StageWork]:
    """`makeNStagesInSerialOrder` in Sparklens's `PQParallelStageSchedulerSuite.scala`: stages 0 to `count`, each reading the one before."""
    return {stage_id: StageWork((1,), (stage_id - 1,) if stage_id else ()) for stage_id in range(count + 1)}


def _fan_in(count: int) -> dict[int, StageWork]:
    """`makeNIndependentStages` there: `count` stages, then one reading them all."""
    return {stage_id: StageWork((1,)) for stage_id in range(count)} | {count: StageWork((1,), tuple(range(count)))}


def _one_job(stages: dict[int, StageWork]) -> RunModel:
    groups, _ = group_jobs([JobSpan(0, None, 0, 0, tuple(stages))])
    return RunModel(0, groups, stages)


# PQParallelStageSchedulerSuite's "2 Jobs running in parallel": jobs 0 and 1 of one execution
# both list stage 0 and overlap, job 2 of it starts after them, and job 3 is another execution's.
_FOUR_JOBS = RunModel(
    3,
    group_jobs(
        [JobSpan(0, 7, 0, 1, (0,)), JobSpan(1, 7, 0, 1, (0,)), JobSpan(2, 7, 2, 3, (1,)), JobSpan(3, 8, 4, 5, (2,))],
    )[0],
    {0: StageWork((1,)), 1: StageWork((1,)), 2: StageWork((1,))},
)


@pytest.mark.parametrize(
    ('model', 'slots', 'expected_ms'),
    [
        (_one_job(_DIAMOND), 1, 24),
        (_one_job(_DIAMOND), 2, 14),
        (_one_job(_DIAMOND), 8, 11),
        *[(_one_job({0: StageWork((1,))}), slots, 1) for slots in range(1, 11)],
        (_one_job({0: StageWork((1,)), 1: StageWork((1,), (0,))}), 1, 2),
        (_one_job(_serial(10)), 1, 11),
        (_one_job(_serial(10)), 2, 11),
        *[
            (_one_job(_fan_in(10)), slots, expected_ms)
            for slots, expected_ms in [(1, 11), (2, 6), (3, 5), (4, 4), (5, 3), (6, 3), (7, 3), (8, 3), (9, 3), (10, 2), (11, 2), (100, 2)]
        ],
        (_one_job(_fan_in(100)), 100, 2),
        (_FOUR_JOBS, 1, 3),
    ],
)
def test_the_replay_gives_the_diamond_dags_times_and_sparklens_suites(model: RunModel, slots: int, expected_ms: int) -> None:
    assert model.estimate_wall_ms(slots) == expected_ms
