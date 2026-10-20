# sparkxray-cli

The `sparkxray` command: read a Spark event log and find out why a run was slow, from a terminal
or a CI job.

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --help
```

Every release is published to that package index, and nothing else is. The command needs Python
3.13 or later. It pins the `sparkxray` library to its own release, so the two always match.

Every command is `sparkxray <resource> <verb> LOG`, where `LOG` is a log file or directory, an
`s3://` URI, or the app id of a log already read:

```bash
sparkxray findings list LOG                  # what slowed the run, costliest first
sparkxray findings list LOG --fail-on critical   # exit 3 on a critical finding, for a CI step
sparkxray reports write LOG --out report.md      # the findings and the run behind them, as Markdown
sparkxray logs diff LOG_A LOG_B                  # what changed between two runs of one job
sparkxray executors simulate LOG                 # the run's wall time at other executor counts
sparkxray stages list LOG --sort wall_ms -n 5
sparkxray tables query LOG "select count(*) from tasks"
```

`--json` prints any read as JSON on stdout, and progress goes to stderr. Under `--json`, or
`reports write --format json`, a refusal prints as one line of JSON on stderr, carrying its error
code. A command line the parser refuses is `E-USAGE-001`. `sparkxray capabilities show --json`
lists every command, rule, table, schema and error code. `sparkxray schemas show NAME` prints the
JSON Schema of one document, whose description names the command printing it. A listing such as
`stages list --json` has none.

Source, documentation and issues: <https://github.com/datapointchris/sparkxray>.
