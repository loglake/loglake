# Shared host-side settings for deploy/docker-compose.yml.
# shellcheck shell=bash

: "${LOGLAKE_COMPOSE_PROJECT:=loglake-dev}"
: "${LOGLAKE_PG_HOST_PORT:=5433}"
: "${LOGLAKE_MINIO_HOST_PORT:=9000}"
: "${LOGLAKE_MINIO_CONSOLE_HOST_PORT:=9001}"
: "${LOGLAKE_INGEST_HOST_PORT:=8088}"
: "${LOGLAKE_OTLP_GRPC_HOST_PORT:=4317}"
: "${LOGLAKE_INGEST_METRICS_HOST_PORT:=9100}"
: "${LOGLAKE_COMPACTOR_METRICS_HOST_PORT:=9101}"
: "${LOGLAKE_QUERY_HOST_PORT:=8089}"
: "${LOGLAKE_QUERY_METRICS_HOST_PORT:=9105}"
: "${LOGLAKE_PROMETHEUS_HOST_PORT:=9090}"

# Which object store the loglake services talk to. `minio` is the default and
# the only store any shipping default selects; `garage` is the opt-in arm of the
# 0.2.0 comparison (task #2958) and lives behind compose profile `garage`.
# MinIO still starts in the garage arm — the loglake services declare a static
# `depends_on` on it — so a matched throughput slice stops it after the stack is
# healthy.
: "${LOGLAKE_OBJECT_STORE:=minio}"
: "${LOGLAKE_GARAGE_HOST_PORT:=3900}"
: "${LOGLAKE_GARAGE_ADMIN_HOST_PORT:=3903}"

# Throwaway loopback credentials, the same posture as minioadmin/minioadmin.
# The key id follows the `GK` + 32 hex shape Garage's own quick start uses.
: "${LOGLAKE_GARAGE_ACCESS_KEY:=GK0123456789abcdef0123456789abcdef}"
: "${LOGLAKE_GARAGE_SECRET_KEY:=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"

# Print the compose profiles a store needs, one per line, or nothing. Pure: the
# store name is the only input, so the preflight regression can drive it.
loglake_compose_store_profiles() {
  case "$1" in
    garage) printf 'garage\n' ;;
    minio) ;;
    *) return 1 ;;
  esac
}

# Print `endpoint<TAB>host_endpoint<TAB>access_key<TAB>secret_key` for a store.
# Pure but for the host port and credential overrides read from the
# environment, which are themselves resolved above.
loglake_compose_store_settings() {
  case "$1" in
    garage)
      printf '%s\t%s\t%s\t%s\n' \
        "http://garage:3900" \
        "http://localhost:$LOGLAKE_GARAGE_HOST_PORT" \
        "$LOGLAKE_GARAGE_ACCESS_KEY" \
        "$LOGLAKE_GARAGE_SECRET_KEY"
      ;;
    minio)
      printf '%s\t%s\t%s\t%s\n' \
        "http://minio:9000" \
        "http://localhost:$LOGLAKE_MINIO_HOST_PORT" \
        minioadmin minioadmin
      ;;
    *) return 1 ;;
  esac
}

if loglake_compose_store_settings "$LOGLAKE_OBJECT_STORE" >/dev/null 2>&1; then
  IFS=$'\t' read -r _loglake_s3_endpoint _loglake_s3_host_endpoint \
    _loglake_s3_access_key _loglake_s3_secret_key \
    < <(loglake_compose_store_settings "$LOGLAKE_OBJECT_STORE")
  : "${LOGLAKE_S3_ENDPOINT:=$_loglake_s3_endpoint}"
  : "${LOGLAKE_S3_HOST_ENDPOINT:=$_loglake_s3_host_endpoint}"
  : "${LOGLAKE_S3_ACCESS_KEY:=$_loglake_s3_access_key}"
  : "${LOGLAKE_S3_SECRET_KEY:=$_loglake_s3_secret_key}"
  unset _loglake_s3_endpoint _loglake_s3_host_endpoint \
    _loglake_s3_access_key _loglake_s3_secret_key
  COMPOSE_PROFILES="${COMPOSE_PROFILES:-$(loglake_compose_store_profiles "$LOGLAKE_OBJECT_STORE")}"
  export COMPOSE_PROFILES
fi
# An unknown store leaves the S3 variables unset on purpose: the preflight
# below names it, and compose's own `${VAR:-default}` keeps MinIO's values, so
# a typo cannot silently point the stack somewhere else.
: "${LOGLAKE_S3_REGION:=us-east-1}"

export LOGLAKE_OBJECT_STORE
export LOGLAKE_GARAGE_HOST_PORT
export LOGLAKE_GARAGE_ADMIN_HOST_PORT
export LOGLAKE_GARAGE_ACCESS_KEY
export LOGLAKE_GARAGE_SECRET_KEY
export LOGLAKE_S3_ENDPOINT
export LOGLAKE_S3_HOST_ENDPOINT
export LOGLAKE_S3_ACCESS_KEY
export LOGLAKE_S3_SECRET_KEY
export LOGLAKE_S3_REGION

export LOGLAKE_COMPOSE_PROJECT
export LOGLAKE_PG_HOST_PORT
export LOGLAKE_MINIO_HOST_PORT
export LOGLAKE_MINIO_CONSOLE_HOST_PORT
export LOGLAKE_INGEST_HOST_PORT
export LOGLAKE_OTLP_GRPC_HOST_PORT
export LOGLAKE_INGEST_METRICS_HOST_PORT
export LOGLAKE_COMPACTOR_METRICS_HOST_PORT
export LOGLAKE_QUERY_HOST_PORT
export LOGLAKE_QUERY_METRICS_HOST_PORT
export LOGLAKE_PROMETHEUS_HOST_PORT

loglake_compose_port_is_listening() {
  local port=$1 hex
  local -a tables=()

  if [ -r /proc/net/tcp ]; then
    tables+=(/proc/net/tcp)
    [ ! -r /proc/net/tcp6 ] || tables+=(/proc/net/tcp6)
    printf -v hex '%04X' "$((10#$port))"
    awk -v port="$hex" '
      $4 == "0A" {
        split($2, address, ":")
        if (toupper(address[2]) == port) found = 1
      }
      END { exit !found }
    ' "${tables[@]}"
    return
  fi

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v ss >/dev/null 2>&1; then
    ss -H -ltn "sport = :$port" 2>/dev/null | grep -q .
  else
    # Bash's /dev/tcp is the last-resort portable probe. It briefly connects
    # to a listener, but avoids letting a compose build run for minutes before
    # Docker reports the collision.
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null
  fi
}

loglake_compose_port_owned_by_project() {
  local port=$1 published
  published=$(docker ps \
    --filter "label=com.docker.compose.project=$LOGLAKE_COMPOSE_PROJECT" \
    --format '{{.Ports}}' 2>/dev/null) || return 1
  grep -Fq ":${port}->" <<<"$published"
}

loglake_compose_port_holder() {
  local port=$1 holder

  holder=$(docker ps --format '{{.Names}}|{{.Ports}}' 2>/dev/null \
    | awk -F '|' -v needle=":${port}->" \
      'index($2, needle) { print "container " $1 " (" $2 ")"; exit }') || true
  if [ -n "$holder" ]; then
    printf '%s\n' "$holder"
    return
  fi

  if command -v lsof >/dev/null 2>&1; then
    holder=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null \
      | awk 'NR == 2 { printf "process %s (pid %s, user %s)", $1, $2, $3 }') || true
  fi
  if [ -z "$holder" ] && command -v ss >/dev/null 2>&1; then
    holder=$(ss -H -ltnp "sport = :$port" 2>/dev/null | head -1) || true
    [ -z "$holder" ] || holder="process $holder"
  fi

  printf '%s\n' "${holder:-an unknown process}"
}

loglake_compose_preflight() {
  local spec variable port holder
  local -a ports=(
    "LOGLAKE_PG_HOST_PORT:$LOGLAKE_PG_HOST_PORT"
    "LOGLAKE_MINIO_HOST_PORT:$LOGLAKE_MINIO_HOST_PORT"
    "LOGLAKE_MINIO_CONSOLE_HOST_PORT:$LOGLAKE_MINIO_CONSOLE_HOST_PORT"
    "LOGLAKE_INGEST_HOST_PORT:$LOGLAKE_INGEST_HOST_PORT"
    "LOGLAKE_OTLP_GRPC_HOST_PORT:$LOGLAKE_OTLP_GRPC_HOST_PORT"
    "LOGLAKE_INGEST_METRICS_HOST_PORT:$LOGLAKE_INGEST_METRICS_HOST_PORT"
    "LOGLAKE_COMPACTOR_METRICS_HOST_PORT:$LOGLAKE_COMPACTOR_METRICS_HOST_PORT"
    "LOGLAKE_QUERY_HOST_PORT:$LOGLAKE_QUERY_HOST_PORT"
    "LOGLAKE_QUERY_METRICS_HOST_PORT:$LOGLAKE_QUERY_METRICS_HOST_PORT"
    "LOGLAKE_PROMETHEUS_HOST_PORT:$LOGLAKE_PROMETHEUS_HOST_PORT"
  )

  if ! loglake_compose_store_profiles "$LOGLAKE_OBJECT_STORE" >/dev/null 2>&1; then
    echo "compose port preflight failed: LOGLAKE_OBJECT_STORE must be 'minio' or 'garage' (got '$LOGLAKE_OBJECT_STORE')" >&2
    return 1
  fi
  if [ "$LOGLAKE_OBJECT_STORE" = garage ]; then
    ports+=(
      "LOGLAKE_GARAGE_HOST_PORT:$LOGLAKE_GARAGE_HOST_PORT"
      "LOGLAKE_GARAGE_ADMIN_HOST_PORT:$LOGLAKE_GARAGE_ADMIN_HOST_PORT"
    )
  fi

  # Every port number is validated before any of them is probed, so a typo is
  # reported as a typo rather than as whatever the earlier ports happen to
  # collide with on this host.
  for spec in "${ports[@]}"; do
    variable=${spec%%:*}
    port=${spec#*:}
    if [[ ! $port =~ ^[1-9][0-9]{0,4}$ ]] || [ "$port" -gt 65535 ]; then
      echo "compose port preflight failed: $variable must be a port from 1 to 65535 (got '$port')" >&2
      return 1
    fi
  done

  for spec in "${ports[@]}"; do
    variable=${spec%%:*}
    port=${spec#*:}
    if loglake_compose_port_is_listening "$port" \
      && ! loglake_compose_port_owned_by_project "$port"; then
      holder=$(loglake_compose_port_holder "$port")
      echo "compose port preflight failed: host port $port ($variable) is already in use by $holder" >&2
      echo "  set $variable to an unused port, or stop the holder before retrying" >&2
      return 1
    fi
  done

  echo "==> compose host-port preflight ok (object store: $LOGLAKE_OBJECT_STORE)"
}
