#!/usr/bin/env bash
# qdb-primary-watchdog-bin.sh
# check primary's health and initiate failover if it fails a few times in a rpw
# Stop/start via questdb.sh, enforce replication.role=primary in server.conf
# Touches a migrate marker before starting.

# Do NOT source this script
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "Please RUN this script, do not source it. Try: bash ${BASH_SOURCE[0]}"
  return 1 2>/dev/null || exit 1
fi

set -o pipefail
LC_ALL=C

# ===================== Config =====================
URL="https://172.31.42.41:9003/"
FAIL_THRESHOLD=5
CHECK_INTERVAL=2
QDB_CTL="/usr/local/questdb/bin/questdb.sh"   # control script
QDB_HOME="${HOME}/.questdb"                    # QuestDB home dir
SERVER_CONF="${QDB_HOME}/conf/server.conf"     # config file
STOP_WAIT_SECS=60
MARKER_FILE="${QDB_HOME}/_migrate_primary"     # marker to touch before start
# ==================================================

log() { printf '[%(%Y-%m-%d %H:%M:%S)T] %s\n' -1 "$*"; }
die() { log "ERROR: $*"; exit 2; }

# Early hard checks
[[ -x "$QDB_CTL" ]] || die "$QDB_CTL not found or not executable"
[[ -d "$QDB_HOME" ]] || die "QDB home '$QDB_HOME' does not exist"
[[ -w "$QDB_HOME" ]] || die "QDB home '$QDB_HOME' is not writable"
[[ -f "$SERVER_CONF" ]] || die "server.conf not found at $SERVER_CONF"
command -v curl >/dev/null || die "curl not found"

get_http_code() {
  local code rc
  code="$(curl -ksS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "$URL" 2>/dev/null)"
  rc=$?
  if (( rc != 0 )); then code="000"; fi
  printf '%s' "$code"
}

stop_qdb() {
  log "Stopping: $QDB_CTL stop -d '$QDB_HOME'"
  "$QDB_CTL" stop -d "$QDB_HOME" || log "Warning: stop returned non-zero (continuing)"

  # Wait until status says "Not running" or timeout
  local t=0 status_out=""
  while (( t < STOP_WAIT_SECS )); do
    status_out="$("$QDB_CTL" status -d "$QDB_HOME" 2>/dev/null || true)"
    if grep -qi "Not running" <<<"$status_out"; then
      log "Confirmed Not running"
      return 0
    fi
    sleep 1
    ((t++))
  done
  log "Warning: did not see 'Not running' after ${STOP_WAIT_SECS}s (last status: ${status_out//[$'\n']/ })"
  return 0
}

ensure_primary_in_conf() {
  # Fail if the key does not exist at all
  if ! grep -Eq '^[[:space:]]*replication\.role[[:space:]]*=' "$SERVER_CONF"; then
    die "replication.role key not found in $SERVER_CONF"
  fi

  # Replace active key line only (skip lines that start with '#')
  sed -Ei '/^[[:space:]]*#/! s/^[[:space:]]*replication\.role[[:space:]]*=[[:space:]]*.*/replication.role=primary/' "$SERVER_CONF" \
    || die "sed failed while updating $SERVER_CONF"

  # Verify result on an active line
  if ! grep -E '^[[:space:]]*replication\.role[[:space:]]*=' "$SERVER_CONF" \
      | grep -Eq '^[[:space:]]*replication\.role[[:space:]]*=[[:space:]]*primary([[:space:]]*(#.*)?)?$'; then
    die "Failed to set replication.role=primary in $SERVER_CONF"
  fi

  log "Config set: replication.role=primary in $SERVER_CONF"
}

start_qdb() {
  # Touch the marker BEFORE starting
  if ! touch "$MARKER_FILE"; then
    die "Cannot touch marker file: $MARKER_FILE"
  fi
  log "Marker touched: $MARKER_FILE"

  # Pass env only to this command (do not export in current shell)
  log "Starting: QDB_REPLICATION_ROLE=primary $QDB_CTL start -d '$QDB_HOME'"
  QDB_REPLICATION_ROLE=primary "$QDB_CTL" start -d "$QDB_HOME"
  log "Start command issued"
}

perform_failover() {
  log "Primary is down"
  log "Starting failover actions..."
  stop_qdb
  ensure_primary_in_conf
  start_qdb
  log "Failover completed"
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
      perform_failover
      exit 0
    fi

    sleep "$CHECK_INTERVAL"
  done
}

trap 'log "Exiting"; exit 0' INT TERM
main
