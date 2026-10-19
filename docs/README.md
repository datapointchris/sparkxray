# sparkxray docs

Why sparkxray is shaped the way it is. The code says what it does; these pages say why, and what
was tried and rejected.

| Page | Holds |
| --- | --- |
| [architecture.md](architecture.md) | the path from a log location to findings; why two distributions, DuckDB, no JVM, and a local copy of S3 logs; how a log is found, refused and decoded; how its tables are built, cached and keyed; what analyzing a log or a bare plan loads |
| [rule-design.md](rule-design.md) | why every ratio carries an absolute floor, how severity and impact are graded, how a rule reads its inputs and names its findings, where each threshold came from and which layer set it, where findings are written, and how the seed rules match the Spark UI |
| [plan-model.md](plan-model.md) | how four plan forms read into one tree, how text trees are read by column, which AQE plan is final, how truncation is flagged, how plan nodes reach the stages that ran them, and how their metrics aggregate as the SQL tab's do |
