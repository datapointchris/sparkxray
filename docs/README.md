# sparkxray docs

Why sparkxray is shaped the way it is. The code says what it does; these pages say why, and what
was tried and rejected.

| Page | Holds |
| --- | --- |
| [architecture.md](architecture.md) | why each step from a log's location to its tables, findings, reports, diffs and simulations is built the way it is, and the alternatives each step rejected |
| [rule-design.md](rule-design.md) | how a rule reads its inputs and grades, names and gates its findings, where its thresholds come from, and why each rule family measures what it does |
| [plan-model.md](plan-model.md) | how every plan form Spark prints reads into one tree, and how that tree's nodes, versions and metrics line up with the stages that ran them and the Spark UI's SQL tab |

Each page's headings state its findings, so a page's `##` lines are its contents.

Two pages outside `docs/` hold design reasons too. [`fixtures/README.md`](../fixtures/README.md)
says how each fixture log seeds the rules, and how a test reads its expectations from the log.
[`benchmarks/parse/README.md`](../benchmarks/parse/README.md) holds the measurement behind parsing
with DuckDB.
