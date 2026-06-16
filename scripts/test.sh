#!/usr/bin/env bash
#
# This script is derived from the compat-test.sh script from opendata-go:
# https://github.com/opendata-oss/opendata-go/blob/aa37f43069c2e512981fa63b2ebcbe2f657f82eb/scripts/compat-test.sh
#
# End-to-end WAL throughput benchmark (write + read, in Mbps).
#
# Drives cmd/s3bench, which exercises the real replication path: writes go
# through wal.Producer (group-commit, segment upload, manifest CAS) and reads
# through wal.Replica (manifest tail, segment fetch/decode, Applier). The
# numbers are what the WAL sustains over S3, not the raw objectstore floor.
#
# By default it spins up a local MinIO container and runs against it. With
# --real-s3 it skips MinIO and benchmarks a real S3 bucket instead.
#
# Prerequisites:
#   - go toolchain
#   - docker (local MinIO mode only)
#
# Usage:
#   ./scripts/test.sh                          # benchmark against a local MinIO
#   ./scripts/test.sh --real-s3 --bucket NAME  # benchmark against real AWS S3
#
# Flags:
#   --real-s3              target real S3 instead of spinning up MinIO
#   --bucket NAME          bucket to use (required with --real-s3)
#   --region REGION        AWS region (default us-east-1)
#   --endpoint URL         custom S3-compatible endpoint (omit for real AWS S3)
#
# In --real-s3 mode credentials come from the ambient environment
# (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must be exported); the script does
# not start or clean up any container.
#
# Benchmark knobs (env vars, override on the command line):
#   workload size
#     BENCH_COUNT          records to append           (default 256)
#     BENCH_SIZE           bytes per record            (default 1 MiB)
#     BENCH_CONCURRENCY    concurrent in-flight Appends (default 32)
#   log location (write and read MUST agree)
#     BENCH_PREFIX         segment object key prefix   (default s3bench/seg)
#     BENCH_MANIFEST       manifest object key         (default s3bench/manifest)
#   producer tuning (write only)
#     BENCH_FLUSH_BYTES    seal a segment at this many buffered bytes (default 8 MiB; 0 = interval only)
#     BENCH_FLUSH_INTERVAL max time a record waits before its segment seals (default 50ms)
#     BENCH_SEGMENT_MAX    cap on a single segment object (default 16 MiB; 0 = one segment per flush)
#     BENCH_UPLOAD_CONC    parallel segment uploads within a flush (default 4)
#     BENCH_MANIFEST_BATCH segment entries coalesced per manifest CAS (default 0 = all of a flush)
#   measurement mode
#     BENCH_LATENCY        true: report per-record commit latency (p50/p90/p99) instead of write throughput (default false)
#
# Throughput vs latency: large BENCH_FLUSH_BYTES / BENCH_FLUSH_INTERVAL and high
# BENCH_CONCURRENCY maximize aggregate throughput by amortizing the per-segment
# and per-manifest-CAS round trips; small flush thresholds and low concurrency
# minimize commit latency per record. See the EXAMPLES section at the bottom.
# Against real S3 raise BENCH_CONCURRENCY (e.g. 128) — per-request latency
# dominates, so a low value badly understates achievable aggregate throughput.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE_DIR="${TMPDIR:-/tmp}/test-cache"

# ---------- argument parsing ----------
REAL_S3=false
ARG_BUCKET=""
ARG_REGION=""
ARG_ENDPOINT=""
while [ $# -gt 0 ]; do
    case "$1" in
        --real-s3)  REAL_S3=true ;;
        --bucket)   ARG_BUCKET="$2"; shift ;;
        --region)   ARG_REGION="$2"; shift ;;
        --endpoint) ARG_ENDPOINT="$2"; shift ;;
        -h|--help)
            sed -n '3,53p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

# ---------- configuration ----------
MINIO_CONTAINER="ingest-compat-minio"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:RELEASE.2025-09-07T16-13-09Z}"
BATCH_COMPRESSION="${BATCH_COMPRESSION:-none}"

if [ "$REAL_S3" = true ]; then
    # Real S3: credentials and region come from the ambient environment.
    if [ -z "${ARG_BUCKET}" ]; then
        echo "ERROR: --real-s3 requires --bucket NAME" >&2
        exit 2
    fi
    if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
        echo "ERROR: --real-s3 needs AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY exported" >&2
        exit 2
    fi
    S3_ENDPOINT="${ARG_ENDPOINT}"
    S3_BUCKET="${ARG_BUCKET}"
    AWS_REGION="${ARG_REGION:-${AWS_REGION:-us-east-1}}"
else
    # Local MinIO defaults.
    S3_ENDPOINT="http://localhost:${MINIO_PORT}"
    S3_BUCKET="${ARG_BUCKET:-compat-test}"
    AWS_ACCESS_KEY_ID="test"
    AWS_SECRET_ACCESS_KEY="testtesttest"
    AWS_REGION="${ARG_REGION:-us-east-1}"
fi

export S3_ENDPOINT S3_BUCKET AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION BATCH_COMPRESSION
export GOCACHE="${GOCACHE:-${CACHE_DIR}/gocache}"
export GOMODCACHE="${GOMODCACHE:-${CACHE_DIR}/gomodcache}"
export GOPATH="${GOPATH:-${CACHE_DIR}/gopath}"

# ---------- helpers ----------
cleanup() {
    echo "--- cleanup ---"
    docker rm -f "${MINIO_CONTAINER}" 2>/dev/null || true
}
[ "$REAL_S3" = true ] || trap cleanup EXIT

log() { echo "=== $* ==="; }

mkdir -p "${GOCACHE}" "${GOMODCACHE}" "${GOPATH}"

wait_for_minio() {
    local attempts=0
    while ! curl -sf "${S3_ENDPOINT}/minio/health/live" >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 30 ]; then
            echo "ERROR: MinIO did not become healthy after 30s"
            exit 1
        fi
        sleep 1
    done
}

# ---------- start MinIO (skipped in --real-s3 mode) ----------
if [ "$REAL_S3" = true ]; then
    log "targeting real S3: bucket=${S3_BUCKET} region=${AWS_REGION} endpoint=${S3_ENDPOINT:-<default AWS>}"
else
    log "starting MinIO"
    docker rm -f "${MINIO_CONTAINER}" 2>/dev/null || true
    docker run -d \
        --name "${MINIO_CONTAINER}" \
        -p "${MINIO_PORT}:9000" \
        -e "MINIO_ROOT_USER=${AWS_ACCESS_KEY_ID}" \
        -e "MINIO_ROOT_PASSWORD=${AWS_SECRET_ACCESS_KEY}" \
        "${MINIO_IMAGE}" server /data

    log "waiting for MinIO to be healthy"
    wait_for_minio
fi

# ---------- build ----------
log "building s3bench"
S3BENCH_BIN="${CACHE_DIR}/s3bench"
( cd "${PROJECT_DIR}" && go build -mod=mod -o "${S3BENCH_BIN}" ./cmd/s3bench )

# Benchmark knobs (override on the command line).
BENCH_COUNT="${BENCH_COUNT:-256}"
BENCH_SIZE="${BENCH_SIZE:-1048576}"      # bytes per record (1 MiB)
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-32}"
BENCH_PREFIX="${BENCH_PREFIX:-s3bench/seg}"
BENCH_MANIFEST="${BENCH_MANIFEST:-s3bench/manifest}"
BENCH_FLUSH_BYTES="${BENCH_FLUSH_BYTES:-8388608}"     # 8 MiB
BENCH_FLUSH_INTERVAL="${BENCH_FLUSH_INTERVAL:-50ms}"
BENCH_SEGMENT_MAX="${BENCH_SEGMENT_MAX:-16777216}"    # 16 MiB
BENCH_UPLOAD_CONC="${BENCH_UPLOAD_CONC:-4}"
BENCH_MANIFEST_BATCH="${BENCH_MANIFEST_BATCH:-0}"
BENCH_LATENCY="${BENCH_LATENCY:-false}"   # true: report per-record commit latency instead of throughput

# ---------- run producer ----------
# With BENCH_LATENCY=true the producer reports per-record commit latency
# (p50/p90/p99) instead of aggregate write throughput; everything else is the
# same so the read step still has a log to tail.
WRITE_FLAGS=()
if [ "${BENCH_LATENCY}" = true ]; then
    WRITE_FLAGS+=(-latency)
    log "write commit latency"
else
    log "write throughput"
fi
"${S3BENCH_BIN}" write \
    -count "${BENCH_COUNT}" \
    -size "${BENCH_SIZE}" \
    -concurrency "${BENCH_CONCURRENCY}" \
    -prefix "${BENCH_PREFIX}" \
    -manifest "${BENCH_MANIFEST}" \
    -flush-bytes "${BENCH_FLUSH_BYTES}" \
    -flush-interval "${BENCH_FLUSH_INTERVAL}" \
    -segment-max-bytes "${BENCH_SEGMENT_MAX}" \
    -upload-concurrency "${BENCH_UPLOAD_CONC}" \
    -manifest-batch "${BENCH_MANIFEST_BATCH}" \
    "${WRITE_FLAGS[@]}"

# ---------- run consumer (read throughput) ----------
log "read throughput"
"${S3BENCH_BIN}" read \
    -prefix "${BENCH_PREFIX}" \
    -manifest "${BENCH_MANIFEST}"

# ---------- EXAMPLES ----------
# Tuning recipes (env vars in front of the script; defaults shown above):
#
# Max aggregate WRITE throughput (amortize per-segment + per-CAS round trips):
#   BENCH_COUNT=4096 BENCH_SIZE=1048576 BENCH_CONCURRENCY=128 \
#   BENCH_FLUSH_BYTES=67108864 BENCH_SEGMENT_MAX=67108864 \
#   BENCH_UPLOAD_CONC=16 BENCH_MANIFEST_BATCH=0 ./scripts/test.sh
#
# Min per-record commit LATENCY, reported as p50/p90/p99 (seal small + often):
#   BENCH_LATENCY=true BENCH_COUNT=512 BENCH_SIZE=4096 BENCH_CONCURRENCY=1 \
#   BENCH_FLUSH_BYTES=0 BENCH_FLUSH_INTERVAL=1ms \
#   BENCH_SEGMENT_MAX=0 BENCH_UPLOAD_CONC=1 BENCH_MANIFEST_BATCH=1 ./scripts/test.sh
#
# Commit latency UNDER LOAD (percentiles with many writers in flight):
#   BENCH_LATENCY=true BENCH_COUNT=4096 BENCH_SIZE=4096 BENCH_CONCURRENCY=128 ./scripts/test.sh
#
# Many small records, throughput-biased (coalesce many entries per manifest CAS):
#   BENCH_COUNT=20000 BENCH_SIZE=512 BENCH_CONCURRENCY=256 \
#   BENCH_FLUSH_BYTES=4194304 BENCH_SEGMENT_MAX=4194304 \
#   BENCH_MANIFEST_BATCH=64 ./scripts/test.sh
#
# Large-object streaming (fat segments, deep upload pipeline):
#   BENCH_COUNT=512 BENCH_SIZE=16777216 BENCH_CONCURRENCY=32 \
#   BENCH_FLUSH_BYTES=134217728 BENCH_SEGMENT_MAX=134217728 \
#   BENCH_UPLOAD_CONC=32 ./scripts/test.sh
#
# Same recipes against real S3 (raise concurrency; latency dominates):
#   BENCH_CONCURRENCY=128 ./scripts/test.sh --real-s3 --bucket my-bucket --region us-east-1
