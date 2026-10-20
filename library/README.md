# sparkxray

Read a Spark event log offline, with no JVM, and find out why a run was slow.

This is the library. The `sparkxray` command is the separate `sparkxray-cli` distribution, so a
project importing the library installs none of the CLI's dependencies.

```bash
uv add --index https://pypi.ichrisbirch.com/simple/ sparkxray
```

Every release is published to that package index, and nothing else is. PyPI does not carry
sparkxray, so a package of that name there is someone else's. The library needs Python 3.11 or
later, so it installs into an AWS Glue 5.0 or 5.1 job:
<https://github.com/datapointchris/sparkxray#run-the-library-inside-an-aws-glue-job>.

Source, documentation and issues: <https://github.com/datapointchris/sparkxray>.
