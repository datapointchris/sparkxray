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
fixtures/        event logs per Spark setting, History Server captures, regenerate.sh, and the jobs
benchmarks/parse/ the parse benchmark, run by hand
```

- **The dependency runs one way: the CLI imports the library, never the reverse.** Typer and rich
  are imported only under `cli/`. Code that imports the library, including inside AWS Glue jobs,
  must never pull them in.
- **A library dependency lands on every consumer's cluster.** Add one only if it ships a cp311
  wheel and the library cannot do without it. It arrives in the change that first imports it, with
  a floor measured against that code.
- **Every error is a class in `sparkxray/errors.py`, with a code callers branch on.** A code is
  never reused or renumbered, and a retired one stays registered. The code goes in with the change
  that first raises it.
- **The CLI holds no logic.** A command parses its arguments, calls one library function and
  renders the result. Anything worth testing lives in the library.
- **The member `pyproject.toml` files carry packaging only.** ruff, mypy and pytest read their
  config from the root file.

## Library code must run on Python 3.11

Glue 5.0 and 5.1 run Python 3.11. So nothing under `library/` may use what arrived later:

- `type` statements and `def f[T]` generic syntax;
- `typing.override` and `itertools.batched`;
- an f-string reusing its own quote character inside a replacement field.

`uv sync` resolves the workspace at 3.13, so nothing local catches these by default. The
`library-on-python-3.11.yml` workflow does, and these commands reproduce its lowest-direct leg:

```bash
uv venv --python 3.11 /tmp/sparkxray-311
uv pip install --python /tmp/sparkxray-311 --resolution lowest-direct ./library
uv pip install --python /tmp/sparkxray-311 pytest
/tmp/sparkxray-311/bin/python -m compileall -q library/src
uv run --only-group dev mypy --python-version 3.11 --python-executable /tmp/sparkxray-311/bin/python library/src
/tmp/sparkxray-311/bin/python -m pytest library
```

Its Glue legs install `.github/glue-preinstalled-modules/5.0.txt` or `5.1.txt` first, then
`./library` with no `--resolution`. The floors in `library/pyproject.toml` are what the
lowest-direct leg installs. Raising a floor is always safe. A floor the code has outgrown fails
there.

## Commands

```bash
uv sync                                  # both members editable, at 3.13
uv run sparkxray --help
uv run mypy .
uv run pytest library                    # the library's tests, over the committed fixtures
uv build --package sparkxray --out-dir dist
uv build --package sparkxray-cli --out-dir dist
fixtures/regenerate.sh [VARIANT...]      # rewrite the fixtures from Spark runs in docker
zstd -dc fixtures/logs/spark-3.5.9/local-*.zstd | python3 fixtures/stage_totals.py
```

A test pins a fixture number from `stage_totals.py`, never from sparkxray's own reading.
`fixtures/README.md` lists each variant and the numbers it seeds.

## Where sparkxray writes

Everything goes under `$XDG_CACHE_HOME/sparkxray/`, or the `cache_dir` a caller passes. Downloaded
objects go under `logs/s3/`, decoded copies under `logs/decoded/`, and Parquet tables under
`tables/<key>/`. Nothing is ever written beside a user's log. An entry for an older version of a
file is removed by a later open once it has gone a day unused, because an `EventLog` opened
earlier still reads it.

DuckDB never reads a URL. Ingest copies S3 objects into the cache with boto3 first, because
DuckDB's `httpfs` extension downloads itself at first use and locked-down networks block that.
boto3 is imported only in `sparkxray/ingest/aws.py`, and every AWS call goes through it. S3 is
verified against a stand-in server, such as moto's, named by `AWS_ENDPOINT_URL_S3`.

## Releases come from conventional commits on main

A `feat` commit cuts a minor release and a `fix` commit a patch, while the version is below 1.0.
One tag releases both distributions. The version is read from the tag at build time, so no file
holds it, and the CLI pins `sparkxray` to that exact version.

## Some files are generated

The keys named by the `managed` list in the root `pyproject.toml`, `.pre-commit-config.yaml`,
`.github/workflows/validate.yml`, `.editorconfig`, `.markdownlint.yaml` and `.shellcheckrc` come
from a shared generator. A hand edit to any of them is reverted on the next regeneration.
`release.yml` and `library-on-python-3.11.yml` are this repo's own.
