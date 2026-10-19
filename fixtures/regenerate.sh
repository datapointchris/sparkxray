#!/usr/bin/env bash
# Regenerate the event log fixtures. Runs the fixture job under each Spark image and each setting
# that changes what Spark writes, then rewrites logs/ and decoded-by-spark/ from the output.
#
#   fixtures/regenerate.sh              every variant
#   fixtures/regenerate.sh VARIANT...   only these, e.g. spark-3.5.9-lz4
#
# Needs docker, zstd, gzip and curl. README.md lists the variants and what each exercises.
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
  glue-5.0
  apache-spark-events
)

# The jobs the reduced variants run. Each lz4, snappy and gzip file Spark writes stays under the
# 500 kB a committed file may be, which the full job's log would not.
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
  docker kill "$name" >/dev/null
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
    mkdir -p "$work/decoded/$variant"
    zstd -19 -q --rm "$log.decoded" -o "$work/decoded/$variant/$name.jsonl.zstd"
  done
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
  local selected=("$@") variant
  [ ${#selected[@]} -gt 0 ] || selected=("${VARIANTS[@]}")
  for variant in "${selected[@]}"; do
    echo "regenerating $variant" >&2
    run_variant "$variant"
    rm -rf "${fixtures:?}/logs/$variant" "${fixtures:?}/decoded-by-spark/$variant"
    mkdir -p "$fixtures/logs"
    mv "$work/$variant" "$fixtures/logs/$variant"
    if [ -d "$work/decoded/$variant" ]; then
      mkdir -p "$fixtures/decoded-by-spark"
      mv "$work/decoded/$variant" "$fixtures/decoded-by-spark/$variant"
    fi
  done
}

main "$@"
