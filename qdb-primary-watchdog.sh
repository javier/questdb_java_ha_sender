#!/usr/bin/env bash
# qdb-primary-watchdog.sh
# This script only works if QuestDB was started via the jar. It needs the following env variables
#   - QDB_ROOT_DIRECTORY
#   - JAVA_QDB_ENT_QDB_SNAPSHOT_JAR
#   - JAVA_QDB_ENT_ENTERPRISE_SNAPSHOT_JAR
#   - JAVA_QDB_ENT_JNI_JAR
# If you are starting QuestDB in a different way, you need to adapt the start/stop parts of the script
# Checks the primary health URL every second.
# After 10 consecutive non-200 responses:
#   - prints that primary is down
#   - stops the QuestDB Java process (matched by "java -p questdb/core/")
#   - exports QDB_REPLICATION_ROLE=primary
#   - touches the migrate marker
#   - restarts QuestDB from /home/ubuntu/questdb-enterprise

# Do NOT source this script
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "Please RUN this script, do not source it. Try: bash ${BASH_SOURCE[0]}"
  return 1 2>/dev/null || exit 1
fi

set -o pipefail
LC_ALL=C

# ===================== Config =====================
URL="https://172.31.42.41:9003/"
FAIL_THRESHOLD=5                   # consecutive failures
CHECK_INTERVAL=2                    # seconds
WORKDIR="/home/ubuntu/questdb-enterprise"
MATCH_EXPR='java -p questdb/core/'  # how we find the Java process to stop
MARKER_FILE="/home/ubuntu/data/disaster_qdb_root/_migrate_primary"
RESTART_LOG="/home/ubuntu/qdb_watchdog_restart.log"
# ==================================================

# EARLY required check (as requested)
if [[ -z "${QDB_ROOT_DIRECTORY:-}" ]]; then
  echo "ERROR: QDB_ROOT_DIRECTORY is not set. Run your prep script first. Aborting."
  exit 2
fi

log() { printf '[%(%Y-%m-%d %H:%M:%S)T] %s\n' -1 "$*"; }

get_http_code() {
  local code rc
  code="$(curl -ksS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "$URL" 2>/dev/null)"
  rc=$?
  if (( rc != 0 )); then code="000"; fi
  printf '%s' "$code"
}

find_pids() {
  # Print matching PIDs, if any
  pgrep -f -- "$MATCH_EXPR" 2>/dev/null || true
}

stop_processes() {
  local pids=("$@")
  if (( ${#pids[@]} == 0 )); then
    log "No matching QuestDB Java process found to stop"
    return 0
  fi
  log "Stopping process(es) ${pids[*]} gracefully..."
  kill -TERM "${pids[@]}" 2>/dev/null || true

  for _ in {1..10}; do
    sleep 1
    local still=()
    local pid
    for pid in "${pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then still+=("$pid"); fi
    done
    if (( ${#still[@]} == 0 )); then
      log "All processes stopped"
      return 0
    fi
  done

  log "Some processes did not stop in time, sending SIGKILL"
  kill -KILL "${pids[@]}" 2>/dev/null || true
  sleep 1
  return 0
}

restart_qdb() {
  # Build module path from env vars
  local module_path="${JAVA_QDB_ENT_QDB_SNAPSHOT_JAR}:${JAVA_QDB_ENT_ENTERPRISE_SNAPSHOT_JAR}:${JAVA_QDB_ENT_JNI_JAR}"

  export QDB_REPLICATION_ROLE=primary
  if ! touch "$MARKER_FILE"; then
    log "ERROR: Failed to create marker file: $MARKER_FILE"
    return 1
  fi
  log "Marker file created: $MARKER_FILE"
  log "Environment set: QDB_REPLICATION_ROLE=primary"

  : > "$RESTART_LOG" 2>/dev/null || true
  log "cd $WORKDIR && java -p '$module_path' -m com.questdb/com.questdb.EntServerMain -d '$QDB_ROOT_DIRECTORY'"

  # Run in a subshell so parent cwd stays unchanged; no PID verification
  (
    cd "$WORKDIR" || exit 1
    nohup java -p "$module_path" -m com.questdb/com.questdb.EntServerMain -d "$QDB_ROOT_DIRECTORY" >>"$RESTART_LOG" 2>&1 &
  )

  log "Restart command issued. Logs: $RESTART_LOG"
  log "Restart command issued. To see server output: tail -f $RESTART_LOG"

  return 0
}

main() {
  local fail_count=0
  log "Starting health check loop for $URL (threshold $FAIL_THRESHOLD, every $CHECK_INTERVAL s)"
  while true; do
    local code
    code="$(get_http_code)"
    if [[ "$code" == "200" ]]; then
      if (( fail_count > 0 )); then
        log "OK 200, resetting fail counter (was $fail_count)"
      fi
      fail_count=0
    else
      ((fail_count++))
      log "Health check failed, HTTP $code (consecutive fails: $fail_count/$FAIL_THRESHOLD)"
    fi

    if (( fail_count >= FAIL_THRESHOLD )); then
      log "Primary is down"
      log "Starting failover actions..."
      mapfile -t pids < <(find_pids)
      stop_processes "${pids[@]}"
      restart_qdb || log "Failover aborted due to errors"
      log "Failover completed"
      exit 0
    fi

    sleep "$CHECK_INTERVAL"
  done
}

trap 'log "Exiting"; exit 0' INT TERM
main
