#!/usr/bin/env bash
#
# Reproduces the "contended commit lock + short-timeout victim" scenario for
# the dsilva_addWaitForOnCommitLock branch using the Cache plugin's WriteCache
# command.
#
# Spawns N parallel WriteCache workers against a leader to keep the commit
# lock hot, then fires one more WriteCache with a very short `Timeout:`
# header. With this branch the victim should respond `555 Timeout` and the
# bedrock log should contain `Timed out after <N>us waiting for commit lock.`
#
# Usage:
#   HOST=127.0.0.1 PORT=8888 LOG_FILE=/var/log/bedrock.log \
#     ./commit_lock_timeout_test.sh
#
# Tunables (env vars):
#   HOST                bedrock host                                (127.0.0.1)
#   PORT                bedrock command port                              (8888)
#   WORKERS             number of concurrent contention workers            (50)
#   WARMUP_SECS         seconds to let workers heat the lock                (2)
#   VICTIM_TIMEOUT_MS   Timeout: header for the victim WriteCache          (50)
#   WORKER_TIMEOUT_MS   Timeout: header for worker WriteCaches          (60000)
#   VALUE_BYTES         size of the WriteCache value payload, in bytes (262144)
#   LOG_FILE            bedrock log to grep for confirmation
#                       (/var/log/syslog)

set -uo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8888}"
WORKERS="${WORKERS:-50}"
WARMUP_SECS="${WARMUP_SECS:-2}"
VICTIM_TIMEOUT_MS="${VICTIM_TIMEOUT_MS:-50}"
WORKER_TIMEOUT_MS="${WORKER_TIMEOUT_MS:-60000}"
VALUE_BYTES="${VALUE_BYTES:-262144}"
LOG_FILE="${LOG_FILE:-/var/log/syslog}"

cleanup() {
    # SIGKILL the worker subshells AND their `nc`/`printf` grandchildren.
    # SIGTERM is unsafe here: bash queues it until the current pipeline
    # returns, and the pipeline is what's wedged.
    if [[ ${#WORKER_PIDS[@]:-0} -gt 0 ]]; then
        for pid in "${WORKER_PIDS[@]}"; do
            pkill -KILL -P "$pid" 2>/dev/null
        done
        kill -KILL "${WORKER_PIDS[@]}" 2>/dev/null
    fi
    pkill -KILL -P $$ 2>/dev/null
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM
WORKER_PIDS=()

# Detect nc flavor (GNU has -q, BSD/macOS does not).
if nc -h 2>&1 | grep -q -- " -q "; then
    NC=(nc -q 1 -w 5)
else
    NC=(nc -w 5)
fi

# A single, in-memory value reused by all workers and the victim.
VALUE="$(head -c "$VALUE_BYTES" /dev/urandom | base64 | head -c "$VALUE_BYTES")"

send_writecache() {
    # Hard-bound each nc invocation. Some netcat builds don't honor -w/-q the
    # way we'd like, so `timeout` is the only guaranteed upper limit.
    local name="$1" timeout_ms="$2"
    printf 'WriteCache\r\nname: %s\r\nvalue: %s\r\nTimeout: %s\r\nConnection: close\r\n\r\n' \
        "$name" "$VALUE" "$timeout_ms" | timeout --signal=KILL 5 "${NC[@]}" "$HOST" "$PORT"
}

worker_loop() {
    local id="$1" i=0
    while :; do
        send_writecache "worker_${id}_$((i++))" "$WORKER_TIMEOUT_MS" >/dev/null 2>&1
    done
}

# 1. Confirm the target is leading. The new code path only runs on the leader
#    (followers escalate writes to the sync thread).
STATUS="$(printf 'Status\r\nConnection: close\r\n\r\n' | "${NC[@]}" "$HOST" "$PORT")"
if ! grep -q '"state"[[:space:]]*:[[:space:]]*"LEADING"' <<<"$STATUS"; then
    echo "ERROR: $HOST:$PORT is not LEADING; aborting." >&2
    echo "First 40 lines of Status response:" >&2
    head -40 <<<"$STATUS" >&2
    exit 1
fi

# 2. Spawn contention.
echo "Spawning $WORKERS WriteCache workers against $HOST:$PORT (value=${VALUE_BYTES}B)..."
for i in $(seq 1 "$WORKERS"); do
    worker_loop "$i" &
    WORKER_PIDS+=($!)
done

# 3. Warm up so the commit lock is consistently held by a worker.
sleep "$WARMUP_SECS"

# 4. Mark log position (if accessible) so we can isolate the victim's lines.
LOG_START_BYTES=0
if [[ -r "$LOG_FILE" ]]; then
    LOG_START_BYTES=$(wc -c <"$LOG_FILE")
fi

# 5. Fire the victim.
VICTIM_NAME="victim_$(date +%s%N)"
echo "Sending victim: name=$VICTIM_NAME Timeout=${VICTIM_TIMEOUT_MS}ms"
VICTIM_RESPONSE="$(send_writecache "$VICTIM_NAME" "$VICTIM_TIMEOUT_MS" || true)"

# 6. Let a few more commits flush through the log, then stop workers.
sleep "$WARMUP_SECS"
cleanup
trap - EXIT INT TERM

# 7. Report.
echo
echo "=== Victim response ==="
echo "$VICTIM_RESPONSE"

VICTIM_STATUS="$(printf '%s' "$VICTIM_RESPONSE" | head -1)"
echo
echo "=== Victim status line ==="
echo "$VICTIM_STATUS"

if [[ -r "$LOG_FILE" ]]; then
    echo
    echo "=== Relevant log lines written after the victim was sent ==="
    tail -c +"$((LOG_START_BYTES + 1))" "$LOG_FILE" | \
        grep -E "Timed out after .* waiting for commit lock|Command 'WriteCache' timed out before commit" || \
        echo "(no matching log lines found)"
else
    echo
    echo "(LOG_FILE=$LOG_FILE not readable; export LOG_FILE=... or read via 'docker logs ...')"
fi

# 8. Pass/fail. A `555` victim status is the contract for path 4.
echo
if [[ "$VICTIM_STATUS" == *"555"* ]]; then
    echo "PASS: victim returned 555 Timeout."
    exit 0
else
    echo "FAIL: victim did not return 555."
    exit 1
fi
