# Architecture

## A log location becomes findings in four library stages

Each stage reads what the one before it wrote. The CLI calls one library function per command and
holds no logic of its own.

```text
 file, directory, rolling directory, s3:// prefix, History Server URL
        │
        ├── ingest ──→ ordered, decoded event lines
        │
        ├── normalize ──→ Parquet tables + manifest.json
        │                 under $XDG_CACHE_HOME/sparkxray/tables/<key>/
        │
        ├── rules ──→ findings: rule id, severity, evidence,
        │             thresholds applied, impact_seconds
        │
        └── reports ──→ Markdown and JSON
```

The tables are keyed by content. The key is the SHA-256 of the ingested file list (URI, ETag or
mtime, size) plus the normalizer's schema version. So a second analysis of one run skips parsing,
and a normalizer upgrade rebuilds.

## The library and the command line are separate distributions

Other code imports the library, including inside AWS Glue jobs. A Glue job importing it must not
pull Typer and rich onto the cluster. So the command line is its own distribution, `sparkxray-cli`,
and the library declares no scripts.

The two also need different Pythons. Glue 5.0 and 5.1 run Python 3.11, so the library supports
3.11. The command line never runs on Glue, and requires 3.13.

Both live in one uv workspace and release from one tag. The CLI's metadata pins
`sparkxray==<its own version>`. So a CLI and a library from different releases cannot install
together.

A uv workspace resolves at one Python: the intersection of its members' `requires-python`. Here
that is 3.13, so nothing in the workspace runs the library at its floor. The
`library-on-python-3.11.yml` workflow does. It installs `./library` unlocked into a 3.11
environment, the way Glue's pip does, then type-checks the library as 3.11 and runs its tests.
Each run installs one of four dependency sets:

- the highest versions;
- the floors the library declares;
- Glue 5.0's preinstalled modules, then the library over them;
- Glue 5.1's, the same way.

A Glue job keeps every preinstalled module that satisfies a floor. So Glue 5.0's numpy 1.26.4 and
pyarrow 17.0.0 are what the library imports there, and neither of the first two sets holds them.

*Rejected:* one distribution with the CLI's dependencies behind an extra. Every CLI install would
have to name the extra, and the CLI would sit on the library's 3.11 floor.

## DuckDB parses the log, so there is no JVM and no custom parser

The benchmark input is a fixture log inflated to 786 MB, holding 126,920 `TaskEnd` events. Over it,
DuckDB's `read_json` with an explicit `columns=` map computed per-stage task aggregates in
0.29–0.30 s on 16 threads. Reading the same log zstd'd took 0.32–0.37 s. On the same file:

- Polars 2.0.0's `scan_ndjson` took 0.36–0.39 s;
- Python with orjson took 2.0–2.1 s.

`benchmarks/parse/README.md` holds every arm and the method.

The explicit `columns=` map is required. Every `SparkListener*` event has its own JSON shape, and
auto-detection unifies them all into one enormous struct. With the map, DuckDB's reader parses in
parallel and drops every field nobody asked for.

*Rejected:* Polars. It parsed in a fifth more time than DuckDB. The rules are SQL over the tables,
and DuckDB is the engine they run in, so Polars would be a second engine beside it.

*Rejected:* a Rust reader. A tuned one might match DuckDB, but only by redoing work DuckDB already
does, and every install would need its build.

*Rejected:* a JVM, including replaying logs through Spark's own History Server. Spark's classes
read logs exactly, but needing Java to read a log on a laptop or in CI is what this tool exists to
avoid. A History Server is an optional input, never a requirement.

## S3 logs are copied to a local cache before DuckDB reads them

DuckDB's Python wheel links `json` and `parquet` statically. Its `httpfs` extension, which reads
`s3://`, is downloaded from `extensions.duckdb.org` the first time a query touches S3. Locked-down
networks block that host, and a Glue job without internet cannot reach it.

So ingest copies objects with boto3 into `$XDG_CACHE_HOME/sparkxray/logs/`, and DuckDB only reads
local files. An object is keyed by its ETag plus its size. An unchanged object is never downloaded
twice, and a log Glue is still appending to is downloaded again. One module makes every AWS call.

## Spark's lz4 and snappy logs need two small decoders

Spark writes a compressed log through its own `CompressionCodec`. Only zstd, Spark 4's default,
writes standard frames that DuckDB opens directly. Spark closes a frame on every flush, so one log
is many frames, and DuckDB reads across them. lz4 writes lz4-java's block stream, and snappy
writes snappy-java's stream. Neither is the standard framing for its codec.

Both are length-prefixed block streams around a standard block codec. So sparkxray carries two
small decoders over cramjam's raw block functions, and decodes those logs into the cache as plain
JSON lines first. lzf is refused with an error that suggests zstd.

A truncated final line is expected. Glue's periodic log backups and killed applications can both
leave one. The reader drops it and records `truncated_tail` in the manifest. An unparsable line
anywhere else is an error naming the file and line.
