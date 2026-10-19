# sparkxray-cli

The `sparkxray` command: read a Spark event log and find out why a run was slow, from a terminal
or a CI job.

```bash
uv tool install --index https://pypi.ichrisbirch.com/simple/ sparkxray-cli
sparkxray --help
```

Every release is published to that package index, and nothing else is. The command needs Python
3.13 or later. It pins the `sparkxray` library to its own release, so the two always match.

Source, documentation and issues: <https://github.com/datapointchris/sparkxray>.
