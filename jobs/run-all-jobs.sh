#!/usr/bin/env sh
set -eu

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
JOBS_DIR="${JOBS_DIR:-/opt/spark/jobs}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-/opt/spark/bin/spark-submit}"
SPARK_SUBMIT_EXTRA_ARGS="${SPARK_SUBMIT_EXTRA_ARGS:-}"

if [ ! -d "$JOBS_DIR" ]; then
    echo "ERROR: JOBS_DIR not found: $JOBS_DIR" >&2
    exit 2
fi

if [ ! -x "$SPARK_SUBMIT_BIN" ]; then
    echo "ERROR: spark-submit not found/executable: $SPARK_SUBMIT_BIN" >&2
    exit 2
fi

echo "Spark master: $SPARK_MASTER_URL"
echo "Jobs dir:    $JOBS_DIR"

# Find jobs deterministically.
# Note: BusyBox find may not support -printf; keep it POSIX-compatible.
jobs="$(find "$JOBS_DIR" -maxdepth 1 -type f -name '*.py' ! -name '_*.py' | sort)"

if [ -z "$jobs" ]; then
    echo "No jobs found in $JOBS_DIR (expected *.py)" >&2
    exit 0
fi

failed=0
for job in $jobs; do
    echo "------------------------------------------------------------"
    echo "Running job: $job"
    # shellcheck disable=SC2086
    "$SPARK_SUBMIT_BIN" --master "$SPARK_MASTER_URL" $SPARK_SUBMIT_EXTRA_ARGS "$job" || failed=$?
    if [ "$failed" -ne 0 ]; then
        echo "ERROR: job failed ($failed): $job" >&2
        exit "$failed"
    fi
    echo "OK: $job"
done

echo "------------------------------------------------------------"
echo "All jobs completed successfully."
