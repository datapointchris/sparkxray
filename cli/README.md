# sparkxray-cli

The `sparkxray` command: read a Spark event log offline, with no JVM, and find out why a run was
slow, from a terminal or a CI job.

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --version     # prints the installed release
```

Every release is published to that package index, and nothing else is. The command needs Python
3.13 or later. It pins the `sparkxray` library to its own release, so the two always match.

Spark writes an event log when `spark.eventLog.enabled=true`. `LOG` below is that log's file or
directory, an `s3://` URI, a History Server's or driver UI's `http(s)://` address, or the app id
of a log already read:

```bash
sparkxray findings list LOG                      # what slowed the run, the most time a fix could recover first
sparkxray findings show LOG FINDING_ID           # one finding: its evidence, its thresholds and how to fix it
sparkxray findings list LOG --fail-on critical   # a CI step: exit 3 only on a critical finding
sparkxray reports write LOG --out report.md      # the findings and the run behind them, as Markdown
sparkxray logs diff GOOD_LOG SLOW_LOG            # what changed in a slow run against a good run of the same job
sparkxray logs ingest LOG --glue-run JOB_NAME:RUN_ID --driver-log stderr.log   # join a Glue run's metrics and the driver's log
```

`sparkxray --help` lists every command, and each command's `--help` says what it takes and prints.
`--json` prints a command's result as JSON on stdout, and progress goes to stderr. A program starts
at `sparkxray capabilities show --json`.

Source, documentation and issues: <https://github.com/datapointchris/sparkxray>.
