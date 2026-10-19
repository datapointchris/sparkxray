# sparkxray

Read a Spark event log and find out why a run was slow.

## Install

The command line:

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --version     # the installed release
```

Every release is published to that package index, and nothing else is. Name it on every install.
PyPI does not carry sparkxray, so a package of that name there is someone else's.

The library, for code that reads event logs itself:

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ sparkxray
```

The library runs on Python 3.11 and later, and the command line on 3.13 and later. The command
line is a separate distribution, so a project importing the library installs no CLI dependencies.

### Inside an AWS Glue job

Glue 5.0 and 5.1 run Python 3.11, so the library installs into a Glue job. Glue 4.0 runs 3.10 and
cannot. Download the wheel from the index alone, and upload it to S3:

```bash
pip download --no-deps --index-url https://pypi.ichrisbirch.com/simple/ --dest wheels sparkxray
aws s3 cp wheels/sparkxray-0.1.0-py3-none-any.whl s3://your-bucket/glue/
```

Then pass its path in the job's parameters. Glue installs the wheel, and its dependencies from
PyPI:

```text
--additional-python-modules  s3://your-bucket/glue/sparkxray-0.1.0-py3-none-any.whl
```

Never give pip the package index as an extra index beside PyPI. pip installs the highest version it
finds on either, so anyone uploading a `sparkxray` to PyPI would run their code in the job.

A job with no internet access installs from a zip of wheels instead. Build it on Amazon Linux 2023,
as AWS's guide to a zip of wheels does. Name the downloaded wheel by its path, so pip never asks
PyPI for sparkxray:

```bash
pip download --dest wheels --platform manylinux2014_x86_64 --python-version 311 \
  --only-binary=:all: wheels/sparkxray-0.1.0-py3-none-any.whl
echo sparkxray > wheels/requirements.txt
zip -r sparkxray-0.1.0.gluewheels.zip wheels/
```

Upload the zip, pass its `s3://` path to `--additional-python-modules`, and pass `--no-index` to
`--python-modules-installer-option`. Glue then resolves every dependency from the zip.

## Layout

| Directory | Holds |
| --- | --- |
| `library/` | the `sparkxray` distribution, which code reading event logs imports |
| `cli/` | the `sparkxray-cli` distribution: the `sparkxray` command over the library |
| `docs/` | why the design is what it is |

Both distributions are released together, from one tag, and the CLI pins the library to its own
release.

## License

MIT
