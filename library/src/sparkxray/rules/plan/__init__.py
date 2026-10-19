"""Rules over a query's physical plan, which run on a bare plan as well as on a log."""

from sparkxray.rules.plan import estimates
from sparkxray.rules.plan import exchanges
from sparkxray.rules.plan import file_sizes
from sparkxray.rules.plan import filters
from sparkxray.rules.plan import joins
from sparkxray.rules.plan import python_udfs
from sparkxray.rules.plan import repeated_work

RULES = (
    *joins.RULES,
    *python_udfs.RULES,
    *exchanges.RULES,
    *filters.RULES,
    *file_sizes.RULES,
    *repeated_work.RULES,
    *estimates.RULES,
)
