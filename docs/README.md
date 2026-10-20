# sparkxray docs

Why sparkxray is shaped the way it is. The code says what it does; these pages say why, and what
was tried and rejected.

| Page | Holds |
| --- | --- |
| [architecture.md](architecture.md) | the path from a log location to findings; why two distributions, DuckDB, no JVM, and a local copy of S3 logs; how a log is found, refused and decoded; how its tables are built, cached and keyed; how a log already read opens by its app id; what analyzing a log or a bare plan loads; how the command's summaries measure stages and executors as the Spark UI does, and where they differ |
| [rule-design.md](rule-design.md) | why every ratio carries an absolute floor, how severity and impact are graded, why the `--fail-on` gate judges findings the listing leaves out, how a rule reads its inputs, names its findings and titles them, where each threshold came from and which layer set it, where findings are written with the analysis that found them, how the seed rules match the Spark UI, and how the plan rules size what ran, count a cached plan once, confirm a repeat, read cut text, match a filter to what was pushed and grade a Python UDF |
| [plan-model.md](plan-model.md) | how four plan forms read into one tree, how text trees are read by column, which AQE plan is final, where cost-mode text's estimates are kept, how truncation is flagged, how plan nodes reach the stages that ran them, and how their metrics aggregate as the SQL tab's do |
