#!/usr/bin/env bash
# Regenerate the event log fixtures. Runs the fixture jobs under each Spark image and each setting
# that changes what Spark writes, then rewrites logs/, decoded-by-spark/, history-server-sql/,
# history-server-rest/, explain/ and driver-logs/ from the output.
#
#   fixtures/regenerate.sh              every variant
#   fixtures/regenerate.sh VARIANT...   only these, e.g. spark-3.5.9-lz4
#
# Needs docker, zstd, gzip, curl and jq, and the plan-rules variants reach PyPI. README.md lists
# the variants and what each exercises.
set -euo pipefail

readonly SPARK_3=apache/spark:3.5.9-python3
readonly SPARK_4=apache/spark:4.2.0-python3
readonly GLUE_5=public.ecr.aws/glue/aws-glue-libs:5
readonly APACHE_TAG=v4.2.0
readonly APACHE_EVENTS=https://raw.githubusercontent.com/apache/spark/$APACHE_TAG/core/src/test/resources/spark-events

readonly VARIANTS=(
  spark-3.5.9
  spark-4.2.0
  spark-3.5.9-lz4
  spark-3.5.9-snappy
  spark-3.5.9-gzip
  spark-4.2.0-rolling
  spark-3.5.9-killed
  spark-4.2.0-killed
  spark-3.5.9-warmup
  spark-3.5.9-plan-shapes
  spark-4.2.0-plan-shapes
  spark-3.5.9-plan-rules
  spark-4.2.0-plan-rules
  spark-3.5.9-stage-rules
  spark-4.2.0-stage-rules
  spark-3.5.9-edited-events
  spark-4.2.0-driver-oom
  glue-5.0
  apache-spark-events
)

# The jobs the reduced variants run. An lz4, snappy or gzip log is committed in its own codec,
# where a plain log is recompressed with zstd -19, so it comes from a reduced job to stay small.
readonly REDUCED=(--only python-udf small-side-join)

fixtures=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
readonly fixtures
work=$(mktemp -d)
readonly work
trap 'rm -rf "$work"' EXIT

# Settings every Spark variant shares. They let each seeded problem reach its rule's default
# floor: no broadcast joins and no coalescing, so a skewed partition stays one task, and a small
# memory fraction, so the wide sort spills. The heap is 2 GiB because skew-join handling splits
# key 0 across four tasks, and merging their dozens of spill files each runs a 1 GiB heap out.
readonly SHARED_CONF=(
  --master 'local[4]'
  --driver-memory 2g
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///out
  --conf spark.sql.autoBroadcastJoinThreshold=-1
  --conf spark.sql.adaptive.coalescePartitions.enabled=false
  --conf spark.sql.adaptive.skewJoin.enabled=false
  --conf spark.sql.shuffle.partitions=16
  --conf spark.memory.fraction=0.1
)

# Settings the plan-shapes job runs under. With broadcast joins off, a join plans as a sort-merge
# join unless the query asks for a broadcast. The log is one plain file on both versions, so the
# History Server and zstd_plain both read it.
readonly PLAN_SHAPES_CONF=(
  --master 'local[2]'
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///out
  --conf spark.eventLog.compress=false
  --conf spark.eventLog.rolling.enabled=false
  --conf spark.sql.autoBroadcastJoinThreshold=-1
  --conf spark.sql.shuffle.partitions=4
)

# Settings the plan-rules job runs under. With broadcast joins off, a join plans as a sort-merge
# join unless the query asks for a broadcast. The heap is 6 GiB because one query broadcasts a build
# side over 1 GiB, which the driver holds while it hashes it into a table about as large again.
readonly PLAN_RULES_CONF=(
  --master 'local[4]'
  --driver-memory 6g
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///out
  --conf spark.eventLog.compress=false
  --conf spark.eventLog.rolling.enabled=false
  --conf spark.sql.autoBroadcastJoinThreshold=-1
  --conf spark.sql.shuffle.partitions=8
)

# Settings the stage-rules job runs under. Two executors of two cores and a 1 GiB heap each let one
# run out of memory and be replaced. Executor peaks go into the log, and shuffle blocks stay
# uncompressed, so a seed's row count sets its shuffle bytes. Skew joins and AQE stay on by default,
# and the job turns each off for the session where a seed needs it.
readonly STAGE_RULES_CONF=(
  --master 'local-cluster[2,2,1024]'
  --conf spark.executor.memory=1g
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///out
  --conf spark.eventLog.logStageExecutorMetrics=true
  --conf spark.sql.autoBroadcastJoinThreshold=-1
  --conf spark.sql.adaptive.coalescePartitions.enabled=false
  --conf spark.sql.shuffle.partitions=16
  --conf spark.shuffle.compress=false
)

# Settings the edited-events job runs under: the stage-rules cluster, with
# spark.sql.shuffle.partitions left unset. edit_events.py then edits the plain log Spark writes.
readonly EDITED_EVENTS_CONF=(
  --master 'local-cluster[2,2,1024]'
  --conf spark.executor.memory=1g
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///out
  --conf spark.eventLog.logStageExecutorMetrics=true
)

# Settings the driver-OOM job runs under: a 512 MiB driver heap, no cap on what one action returns,
# and Spark's structured logging, which writes the driver's log to stderr as JSON lines.
readonly DRIVER_OOM_CONF=(
  --master 'local[2]'
  --driver-memory 512m
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=file:///out
  --conf spark.log.structuredLogging.enabled=true
  --conf spark.driver.maxResultSize=0
)

# A pandas UDF needs pyarrow and pandas, and neither Spark image ships them. Spark 4.2 refuses a
# pyarrow under 18.0.0 and a pandas under 2.2.0, and these releases still install on Python 3.10.
readonly PANDAS_UDF_PACKAGES='pyarrow==18.1.0 pandas==2.2.3 numpy==1.26.4'

# container_args IMAGE OUT: the docker flags every run shares, ending with the image and the
# script that runs the command as root and hands /out and /data back to the caller.
container_args() {
  printf '%s\n' --user 0 -e "OWNER=$(id -u):$(id -g)" \
    -v "$fixtures/job:/job:ro" -v "$2:/out" -v "$work/data:/data" --entrypoint sh "$1" \
    /job/run-and-hand-back-output.sh
}

# submit IMAGE OUT SPARK_SUBMIT ARGS...: run one spark-submit with OUT mounted at /out.
submit() {
  local image=$1 out=$2
  shift 2
  mkdir -p "$out" "$work/data"
  local args
  mapfile -t args < <(container_args "$image" "$out")
  docker run --rm "${args[@]}" "$@"
}

# submit_and_kill IMAGE OUT JOBS SPARK_SUBMIT ARGS...: run in the background, and kill the
# container a few seconds after the driver starts its JOBS-th job, mid-way through that job's
# task events. The driver's console is read rather than the log, because Spark creates the log
# readable by its own user only.
submit_and_kill() {
  local image=$1 out=$2 jobs=$3
  shift 3
  mkdir -p "$out" "$work/data"
  local args name="sparkxray-fixture-killed-$$"
  mapfile -t args < <(container_args "$image" "$out")
  docker run -d --name "$name" "${args[@]}" "$@" >/dev/null
  until [ "$(docker logs "$name" 2>&1 | grep -cE 'DAGScheduler: Got (map stage )?job')" -ge "$jobs" ]; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$name")" != true ]; then
      docker logs "$name" 2>&1 | tail -20 >&2
      docker rm "$name" >/dev/null
      echo "regenerate.sh: the run stopped before the driver started $jobs jobs" >&2
      exit 1
    fi
    sleep 1
  done
  sleep 5
  if ! docker kill "$name" >/dev/null; then
    docker rm "$name" >/dev/null
    echo "regenerate.sh: the run ended within 5 seconds of the driver starting $jobs jobs, before it could be killed" >&2
    exit 1
  fi
  docker rm "$name" >/dev/null
  docker run --rm --user 0 -v "$out:/out" -v "$work/data:/data" --entrypoint chown "$image" \
    -R "$(id -u):$(id -g)" /out /data
}

# zstd_plain FILE: replace one plain log with its zstd copy, named the way Spark names a log it
# compressed itself: local-1 becomes local-1.zstd, and local-1.inprogress local-1.zstd.inprogress.
zstd_plain() {
  local file=$1 target
  case $file in
    *.inprogress) target=${file%.inprogress}.zstd.inprogress ;;
    *) target=$file.zstd ;;
  esac
  zstd -19 -q --rm "$file" -o "$target"
}

# decode_with_spark IMAGE OUT CODEC: write what Spark's own codec reads back from each log in OUT,
# under decoded-by-spark/, compressed so it fits beside the log.
decode_with_spark() {
  local image=$1 out=$2 codec=$3 variant
  variant=$(basename "$out")
  local log name
  for log in "$out"/*."$codec"; do
    name=$(basename "$log")
    submit "$image" "$out" /opt/spark/bin/spark-submit --master 'local[1]' \
      /job/spark_decode.py "$codec" "/out/$name" "/out/$name.decoded"
    mkdir -p "$work/decoded-by-spark/$variant"
    zstd -19 -q --rm "$log.decoded" -o "$work/decoded-by-spark/$variant/$name.jsonl.zstd"
  done
}

# capture_history_server IMAGE OUT [rest]: start IMAGE's History Server over the plain logs in OUT,
# and save each application's SQL endpoint, with node details and plan text, under
# history-server-sql/, compacted and zstd'd. With rest, save instead every answer a rebuild from
# REST reads, under history-server-rest/.
capture_history_server() {
  local image=$1 out=$2 what=${3:-sql} variant name="sparkxray-fixture-history-$$" address apps='' app
  variant=$(basename "$out")
  # Spark creates each log readable by its own user only, and the server runs as that user.
  chmod -R a+rX "$out"
  docker run -d --name "$name" -p 127.0.0.1::18080 -v "$out:/logs:ro" \
    -e SPARK_HISTORY_OPTS='-Dspark.history.fs.logDirectory=file:///logs -Dspark.history.fs.update.interval=1s' \
    --entrypoint /opt/spark/bin/spark-class "$image" org.apache.spark.deploy.history.HistoryServer >/dev/null
  address=$(docker port "$name" 18080/tcp)
  for _ in $(seq 1 120); do
    apps=$(curl -sf "http://$address/api/v1/applications" || true)
    if [ -n "$apps" ] && [ "$(jq length <<<"$apps")" -gt 0 ]; then
      break
    fi
    apps=''
    sleep 1
  done
  if [ -z "$apps" ]; then
    docker logs "$name" 2>&1 | tail -20 >&2
    docker rm -f "$name" >/dev/null
    echo "regenerate.sh: the History Server listed no application within two minutes" >&2
    exit 1
  fi
  mkdir -p "$work/history-server-$what/$variant"
  while IFS= read -r app; do
    if [ "$what" = rest ]; then
      uv --directory "$fixtures/.." run --quiet python "$fixtures/record_history_server_rest.py" \
        "http://$address" "$app" "$work/history-server-rest/$variant/$app.jsonl.zstd"
    else
      curl -fsS "http://$address/api/v1/applications/$app/sql?details=true&planDescription=true&length=1000" \
        | jq -c . | zstd -19 -q -o "$work/history-server-sql/$variant/$app.json.zstd"
    fi
  done < <(jq -r '.[].id' <<<"$apps")
  docker rm -f "$name" >/dev/null
}

run_variant() {
  local variant=$1 out="$work/$1"
  case $variant in
    spark-3.5.9)
      submit "$SPARK_3" "$out" /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" /job/eventlog_job.py /data
      zstd_plain "$out"/local-*
      ;;
    spark-4.2.0)
      submit "$SPARK_4" "$out" /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" /job/eventlog_job.py /data
      ;;
    spark-3.5.9-lz4 | spark-3.5.9-snappy)
      local codec=${variant##*-}
      submit "$SPARK_3" "$out" /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" \
        --conf spark.eventLog.compress=true --conf "spark.eventLog.compression.codec=$codec" \
        /job/eventlog_job.py /data "${REDUCED[@]}"
      decode_with_spark "$SPARK_3" "$out" "$codec"
      ;;
    spark-3.5.9-gzip)
      submit "$SPARK_3" "$out" /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" /job/eventlog_job.py /data "${REDUCED[@]}"
      gzip -9 "$out"/local-*
      ;;
    spark-4.2.0-rolling)
      # Rolling counts the bytes Spark writes, compressed or not, and refuses a size under 10 MiB.
      # So Spark writes plain files padded past that size, and each is compressed here after.
      submit "$SPARK_4" "$out" /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" \
        --conf spark.eventLog.compress=false --conf spark.eventLog.rolling.enabled=true \
        --conf spark.eventLog.rolling.maxFileSize=10m \
        /job/eventlog_job.py /data --only small-side-join --rolling-padding 60
      local file
      for file in "$out"/eventlog_v2_*/events_*; do
        zstd_plain "$file"
      done
      ;;
    spark-3.5.9-killed)
      submit_and_kill "$SPARK_3" "$out" 3 /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" /job/eventlog_job.py /data
      zstd_plain "$out"/local-*.inprogress
      ;;
    spark-4.2.0-killed)
      submit_and_kill "$SPARK_4" "$out" 3 /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" /job/eventlog_job.py /data
      ;;
    spark-3.5.9-warmup)
      submit "$SPARK_3" "$out" /opt/spark/bin/spark-submit "${SHARED_CONF[@]}" \
        --conf spark.sql.adaptive.skewJoin.enabled=true /job/eventlog_job.py /data --warmup
      zstd_plain "$out"/local-*
      ;;
    spark-3.5.9-plan-shapes | spark-4.2.0-plan-shapes)
      local image=$SPARK_3
      [ "$variant" = spark-3.5.9-plan-shapes ] || image=$SPARK_4
      submit "$image" "$out" /opt/spark/bin/spark-submit "${PLAN_SHAPES_CONF[@]}" /job/plan_shapes_job.py /data/plan-shapes
      capture_history_server "$image" "$out"
      zstd_plain "$out"/local-*
      ;;
    spark-3.5.9-plan-rules | spark-4.2.0-plan-rules)
      local image=$SPARK_3
      [ "$variant" = spark-3.5.9-plan-rules ] || image=$SPARK_4
      submit "$image" "$out" sh -c \
        "pip install --quiet --disable-pip-version-check $PANDAS_UDF_PACKAGES && exec /opt/spark/bin/spark-submit \"\$@\"" \
        spark-submit "${PLAN_RULES_CONF[@]}" /job/plan_rules_job.py /data/plan-rules /data/plan-rules-estimates.json
      mkdir -p "$work/explain/$variant"
      mv "$work/data/plan-rules-estimates.json" "$work/explain/$variant/cost-mode.json"
      zstd_plain "$out"/local-*
      ;;
    spark-3.5.9-stage-rules)
      submit "$SPARK_3" "$out" /opt/spark/bin/spark-submit "${STAGE_RULES_CONF[@]}" /job/stage_rules_job.py
      capture_history_server "$SPARK_3" "$out" rest
      zstd_plain "$out"/app-*
      ;;
    spark-4.2.0-stage-rules)
      submit "$SPARK_4" "$out" /opt/spark/bin/spark-submit "${STAGE_RULES_CONF[@]}" /job/stage_rules_job.py
      ;;
    spark-3.5.9-edited-events)
      submit "$SPARK_3" "$out" /opt/spark/bin/spark-submit "${EDITED_EVENTS_CONF[@]}" /job/edited_events_job.py
      local log
      for log in "$out"/app-*; do
        python3 "$fixtures/edit_events.py" <"$log" >"$log.edited"
        mv "$log.edited" "$log"
      done
      zstd_plain "$out"/app-*
      ;;
    spark-4.2.0-driver-oom)
      # The run fails by design. Its stderr is the driver's structured log, kept under driver-logs/.
      mkdir -p "$work/driver-logs/$variant"
      if submit "$SPARK_4" "$out" /opt/spark/bin/spark-submit "${DRIVER_OOM_CONF[@]}" /job/driver_oom_job.py \
        2>"$work/driver-logs/$variant/stderr.log"; then
        echo "regenerate.sh: $variant was to run its driver out of memory, and it succeeded" >&2
        exit 1
      fi
      zstd -19 -q --rm "$work/driver-logs/$variant/stderr.log" -o "$work/driver-logs/$variant/stderr.log.zstd"
      ;;
    glue-5.0)
      # Glue's own spark-defaults.conf stays in force, apart from where the log goes.
      submit "$GLUE_5" "$out" /usr/lib/spark/bin/spark-submit --master 'local[4]' \
        --conf spark.eventLog.dir=file:///out /job/eventlog_job.py /data "${REDUCED[@]}"
      zstd_plain "$out"/local-*
      ;;
    apache-spark-events)
      # Apache Spark's own test logs at a pinned tag, zstd'd like every plain log here. Compressed,
      # no text hook can rewrite them, and each decompresses to Apache's bytes exactly.
      local name rolling=eventlog_v2_local-1766844910796
      mkdir -p "$out/$rolling"
      for name in application_1553914137147_0018 local-1430917381535_1 local-1430917381535_2 app-20200706201101-0003; do
        curl -fsSL --retry 3 -o "$out/$name" "$APACHE_EVENTS/$name"
        zstd_plain "$out/$name"
      done
      for name in events_1_local-1766844910796 appstatus_local-1766844910796 .appstatus_local-1766844910796.crc; do
        curl -fsSL --retry 3 -o "$out/$rolling/$name" "$APACHE_EVENTS/$rolling/$name"
      done
      zstd_plain "$out/$rolling/events_1_local-1766844910796"
      ;;
    *)
      echo "regenerate.sh: no variant named $variant; the variants are: ${VARIANTS[*]}" >&2
      exit 2
      ;;
  esac
}

main() {
  local selected=("$@") variant beside
  [ ${#selected[@]} -gt 0 ] || selected=("${VARIANTS[@]}")
  for variant in "${selected[@]}"; do
    echo "regenerating $variant" >&2
    run_variant "$variant"
    rm -rf "${fixtures:?}/logs/$variant"
    mkdir -p "$fixtures/logs"
    mv "$work/$variant" "$fixtures/logs/$variant"
    for beside in decoded-by-spark history-server-sql history-server-rest explain driver-logs; do
      rm -rf "${fixtures:?}/$beside/$variant"
      if [ -d "$work/$beside/$variant" ]; then
        mkdir -p "$fixtures/$beside"
        mv "$work/$beside/$variant" "$fixtures/$beside/$variant"
      fi
    done
  done
}

main "$@"
