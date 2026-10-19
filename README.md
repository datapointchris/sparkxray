# sparkxray

Read a Spark event log and find out why a run was slow.

sparkxray reads the event logs the Spark UI replays, offline and with no JVM. It reads logs from
Scala, Java, SQL and PySpark applications alike. It loads each log into Parquet tables that DuckDB
can query.

A catalog of rules then looks for task skew, spill, poor join choices, idle or oversized executors
and small files. Each finding names the stage and plan node that paid for it, the evidence, and a
fix.

## Install

The command line:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --version     # the release, and the commit it was built from
```

Every release is published to that package index, and nothing else is. Name it on the install.
Without it uv asks PyPI, which does not carry sparkxray.

The library, for code that reads event logs itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ sparkxray
```

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The command
line is a separate distribution, so a project importing the library installs no CLI dependencies.

### Inside an AWS Glue job

Glue 5.0 and 5.1 run Python 3.11, so the library installs into a Glue job. Glue 4.0 runs 3.10 and
cannot. Install it through the job's parameters:

```text
--additional-python-modules        sparkxray
--python-modules-installer-option  --index-url https://pypi.ichrisbirch.com/simple/ --extra-index-url https://pypi.org/simple/
```

The first index serves sparkxray, and the second serves its dependencies.

A job with no internet access installs from S3 instead. Zip the sparkxray wheel and every
dependency's wheel into one file whose name ends in `.gluewheels.zip`, and upload it. Pass its
`s3://` path to `--additional-python-modules`, and `--no-index` to
`--python-modules-installer-option`. Glue then resolves every dependency from the zip.

## Layout

| Directory | Holds |
| --- | --- |
| `library/` | the `sparkxray` distribution: ingest, tables, plan parser, rules and reports |
| `cli/` | the `sparkxray-cli` distribution: the `sparkxray` command over the library |
| `docs/` | why the design is what it is |

Both distributions are released together, from one tag, and the CLI pins the library to its own
release.

## License

MIT
