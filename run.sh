#!/usr/bin/env zsh
set -euo pipefail

# Machine-local knobs
N=${N:-16}                              # processes on this machine
COUNT_PER=${COUNT_PER:-2000000}         # tx per process
BATCH_SIZE=${BATCH_SIZE:-45000}         # tx per stream
INFLIGHT_TOTAL=${INFLIGHT_TOTAL:-96}    # total streams across all procs
INFLIGHT_PER=$(( INFLIGHT_TOTAL / N ))  # streams per proc
ELASTIC_INTERVAL=${ELASTIC_INTERVAL:-250}  # ms
START=$(( $(date +%s) + 20 ))           # start in 20s

# Unique base offset for this machine (optional if also running on other hosts)
MACHINE_INDEX=${MACHINE_INDEX:-0}
MACHINE_BASE_OFFSET=$(( MACHINE_INDEX * N * COUNT_PER ))

SHARD_A=${SHARD_A:-<server:9000>}
SHARD_B=${SHARD_B:-<server:9000>}

echo "Starting ${N} clients at $(date -r $START) with inflight=${INFLIGHT_PER} batch=${BATCH_SIZE}"

pids=()
for i in $(seq 0 $((N-1))); do
  OFF=$(( MACHINE_BASE_OFFSET + i * COUNT_PER ))
  JITTER=$(( (RANDOM % 5) ))    # 0–4s jitter per process
  ./target/release/client \
    --shard-0 "${SHARD_A}" \
    --shard-1 "${SHARD_B}" \
    --count ${COUNT_PER} \
    --offset ${OFF} \
    --tps 0 \
    --batch-size ${BATCH_SIZE} \
    --inflight ${INFLIGHT_PER} \
    --elastic \
    --elastic-interval-ms ${ELASTIC_INTERVAL} \
    --start-at $(( START + JITTER )) \
    > client_${i}.log 2>&1 &

  pids+=($!)
done

trap "echo 'Stopping...'; kill ${pids[*]} 2>/dev/null || true" INT TERM
wait