# sparkxray docs

Why sparkxray is shaped the way it is. The code says what it does; these pages say why, and what
was tried and rejected.

| Page | Holds |
| --- | --- |
| [architecture.md](architecture.md) | the path from a log location to findings; why two distributions, DuckDB, no JVM, and a local copy of S3 logs |
| [rule-design.md](rule-design.md) | why every ratio carries an absolute floor, how severity and impact are graded, and where each threshold came from |
| [plan-model.md](plan-model.md) | how four plan forms read into one tree, which AQE plan is final, and how plan nodes reach the stages that ran them |
