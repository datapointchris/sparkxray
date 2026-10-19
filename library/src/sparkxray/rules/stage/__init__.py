"""Rules over a stage's tasks: skew, partition sizes, spill, overhead, imbalance and the attempts it threw away."""

from sparkxray.rules.stage import failures
from sparkxray.rules.stage import imbalance
from sparkxray.rules.stage import overhead
from sparkxray.rules.stage import partitions
from sparkxray.rules.stage import skew
from sparkxray.rules.stage import spill

RULES = (
    *skew.RULES,
    *partitions.RULES,
    *spill.RULES,
    *overhead.RULES,
    *imbalance.RULES,
    *failures.RULES,
)
