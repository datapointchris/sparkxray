# sparkxray

Reads Spark event logs offline, with no JVM, and finds why a run was slow. `docs/README.md`
indexes why the design is what it is. Read it before changing ingest, the tables, the plan
model or how a rule grades.

## Two distributions share one uv workspace

```text
pyproject.toml   the workspace, and every tool's config for both members
library/         sparkxray: imported as `sparkxray`, Python >=3.11, no scripts
cli/             sparkxray-cli: imported as `sparkxray_cli`, Python >=3.13, the `sparkxray` command
docs/            the design pages
fixtures/        the logs the tests read, what Spark and a History Server read back from them, and the jobs and scripts that write them
benchmarks/parse/ the parse benchmark, run by hand
```

- **The dependency runs one way: the CLI imports the library, never the reverse.** Typer and rich
  are imported only under `cli/`. Code that imports the library, including inside AWS Glue jobs,
  must never pull them in.
- **A library dependency lands on every consumer's cluster.** Add one only if the library cannot
  do without it. It must install on CPython 3.11 from a wheel: a `cp311` build, or a pure-Python
  `py3-none-any` one, as boto3 and httpx2 ship. It arrives in the change that first imports it,
  with a floor measured against that code.
- **Every error is a class in `sparkxray/errors.py`, with a code callers branch on.** A code is
  never reused or renumbered, and a retired one stays registered. The code goes in with the change
  that first raises it.
- **A JSON document a program keeps or checks has a schema in `sparkxray/schemas/`.**
  `sparkxray schemas list` names every one. A command's listing, such as `stages list --json`, has
  none. A new document adds its file there in the change that creates
  it, with every object closed and a description naming the command that prints it. `schemas list`
  and `capabilities show` find it by its file name. Every number it describes ends in its unit, which
  `library/tests/test_schemas.py` checks. The `tables` and `errors` schemas are built from the
  registries in `sparkxray/schemas/__init__.py`, so a table or an error needs no schema edit.
- **The CLI holds no logic.** A command parses its arguments, calls the library and renders the
  result. It composes library calls and computes nothing itself. Anything worth testing lives in
  the library. The per-resource reads live in `sparkxray.summaries`. `sparkxray_cli/render.py`
  holds the stdout and stderr consoles, and every table, JSON, note and error printer. A command
  whose output is no table, such as `plans show`'s tree or `rules show`'s lesson, builds its rich
  object in its own module and prints it on `render.stdout`.
- **A rules module never imports `sparkxray.summaries` as it loads.** `summaries` imports
  `tables.relations`, which imports `rules.findings`, so the import would cycle. SQL a rule and a
  summary share lives in `tables/registry.py`, as `EXECUTOR_TIME_CTES` does.
- **A registry column states what it holds, and a column whose name ends in a unit has a glossary
  entry.** The description is the fourth field of its `_columns` tuple in `tables/registry.py`.
  The entry is its `table.column` key in `_COLUMN_SOURCES` in `sparkxray/metric_glossary.py`,
  which `metrics list` reads. A meaning is quoted from the Spark docs, read from Spark's source, or
  taken from AWS's reference for a Glue table's column, and cited by page or by file and line. A
  column sparkxray derives from one event field cites that field. What comes from an Apache-licensed
  project is credited in `library/NOTICE`. Never write a meaning from the metric's name.
- **A library refusal reaches the terminal through the root group.** `SparkxrayGroup` in
  `sparkxray_cli/main.py` prints it on stderr and exits with the error's own `exit_code`. So a
  command never catches a `SparkxrayError` itself.
- **The member `pyproject.toml` files carry packaging only.** ruff, mypy and pytest read their
  config from the root file.

## Library code must run on Python 3.11

Glue 5.0 and 5.1 run Python 3.11. So nothing under `library/` may use what arrived later. These
three are the easiest to reach for:

- `type` statements and `def f[T]` generic syntax;
- `typing.override` and `itertools.batched`;
- an f-string reusing its own quote character inside a replacement field.

`compileall` under 3.11 catches later syntax, and `mypy --python-version 3.11` catches a later
standard-library name. `uv sync` resolves the workspace at 3.13, so nothing local catches these
by default. The `library-on-python-3.11.yml` workflow does, and these commands reproduce its
lowest-direct leg:

```bash
uv venv --python 3.11 /tmp/sparkxray-311
uv pip install --python /tmp/sparkxray-311 --resolution lowest-direct ./library
uv pip install --python /tmp/sparkxray-311 pytest jsonschema
/tmp/sparkxray-311/bin/python -m compileall -q library/src
uv run --only-group dev mypy --python-version 3.11 --python-executable /tmp/sparkxray-311/bin/python library/src
/tmp/sparkxray-311/bin/python -m pytest library
```

Its Glue legs install `.github/glue-preinstalled-modules/5.0.txt` or `5.1.txt` first, then
`./library` with no `--resolution`. The floors in `library/pyproject.toml` are what the
lowest-direct leg installs. A floor the code has outgrown fails there. Raising a floor never fails
a leg. A floor past a Glue release's preinstalled version makes installing the library replace that
module inside the job, and no leg catches that. The lists under `.github/glue-preinstalled-modules/`
give each release's versions.

## Every check and build runs from the repo root

```bash
uv sync                                  # both members editable, at 3.13
uv run sparkxray --help
uv run mypy .
uv run pytest library                    # the library's tests, over the committed fixtures
uv run pytest cli                        # the command: exit statuses, the error envelope, capabilities
uv build --package sparkxray --out-dir dist
uv build --package sparkxray-cli --out-dir dist
fixtures/regenerate.sh [VARIANT...]      # rewrite the fixtures from Spark runs in docker
uv run python fixtures/write_lesson_examples.py   # after a rule or fixture changes
zstd -dc fixtures/logs/spark-3.5.9/local-*.zstd | python3 fixtures/stage_totals.py
```

A test pins a fixture number from `stage_totals.py`, never from sparkxray's own reading.
`fixtures/README.md` lists each variant and the numbers it seeds.

## Everything sparkxray writes goes under its cache directory

Everything goes under `$XDG_CACHE_HOME/sparkxray/`, or the `cache_dir` a caller passes. Downloaded
objects go under `logs/s3/`, what a History Server or driver UI served, or the log rebuilt from
it, under `logs/rest/`, decoded copies under `logs/decoded/`, and Parquet tables under
`tables/<source hash>/<key>/`. `analyze` writes `findings.parquet` into the tables' directory, and
`enrich` writes each table it joins there. Nothing is ever written beside a user's log. An entry
for an older version of a file is removed by a later open once it has gone a day unused, because
an `EventLog` opened earlier still reads it.

Decoded copies and tables are keyed on the installed sparkxray version. So a release rebuilds
both, and a decoder or build change bumps no constant. A working tree's installed version moves
only when `uv sync` reinstalls it. So after editing a decoder or the build, pass a fresh
`cache_dir` or clear the cache. A server's download or REST rebuild of a finished attempt is keyed
on the attempt alone, and survives both. So after editing `ingest/events_from_rest.py`, clear
`logs/rest/` as well. `SCHEMA_VERSION` in `tables/manifest.py` and `ingest/manifest.py` is bumped
by hand, when a manifest field or a table is removed or changes meaning.

DuckDB never reads a URL. Ingest copies S3 objects into the cache with boto3 first, because
DuckDB's `httpfs` extension downloads itself at first use and locked-down networks block that.
boto3 is imported only in `sparkxray/ingest/aws.py`, and every AWS call goes through it: S3,
CloudWatch and CloudWatch Logs. The suite starts no stand-in for them. So verify a change there
by hand against one, such as moto's server, named by `AWS_ENDPOINT_URL_S3`,
`AWS_ENDPOINT_URL_CLOUDWATCH` or `AWS_ENDPOINT_URL_CLOUDWATCH_LOGS`.
Nothing is ever written to AWS. httpx2 is
imported only in `sparkxray/ingest/rest_client.py`, and every request to a Spark server goes
through it. A server is verified against a History Server started in the `apache/spark` image, or
against the answers `fixtures/record_history_server_rest.py` saved from one.

## Releases come from conventional commits on main

A `feat` commit cuts a minor release and a `fix` commit a patch, while the version is below 1.0.
A breaking change cuts a minor release too, because `major_on_zero` is false in the root
`pyproject.toml`.
One tag releases both distributions. The version is read from the tag at build time, so no file
holds it, and the CLI pins `sparkxray` to that exact version.

## Some files are generated

The keys named by the `managed` list in the root `pyproject.toml`, `.pre-commit-config.yaml`,
`.github/workflows/validate.yml`, `.editorconfig`, `.markdownlint.yaml` and `.shellcheckrc` come
from a shared generator. A hand edit to any of them is reverted on the next regeneration.
`release.yml` and `library-on-python-3.11.yml` are this repo's own.
