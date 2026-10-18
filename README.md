# sparkxray

Read a Spark event log and find out why a run was slow.

sparkxray reads the event logs the Spark UI replays, offline and with no JVM. It reads logs from
Scala, Java, SQL and PySpark applications alike. It loads each log into Parquet tables that DuckDB
can query.

A catalog of rules then looks for task skew, spill, poor join choices, idle or oversized executors
and small files. Each finding names the stage and plan node that paid for it, the evidence, and a
fix.

## License

MIT
