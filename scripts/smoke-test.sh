#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly READINESS_TIMEOUT_SECONDS=120
readonly OBSERVATION_TIMEOUT_SECONDS=180
readonly MEMORY_PERCENT_LIMIT=80

cd "$PROJECT_ROOT"

compose() {
  docker compose "$@"
}

# A `readonly`/`local` assignment masks the exit status of its command substitution, so `set -e`
# cannot catch a missing key here. Resolve into a plain variable and check explicitly, otherwise a
# typo'd key silently yields an empty port and a 120s timeout against http://localhost:/actuator/...
dotenv_value() {
  local key=$1
  local name
  local value

  while IFS='=' read -r name value; do
    if [[ $name == "$key" ]]; then
      printf '%s' "$value"
      return 0
    fi
  done < .env

  return 1
}

require_port() {
  local key=$1
  local value="${!key:-}"

  if [[ -z $value ]]; then
    if ! value="$(dotenv_value "$key")" || [[ -z $value ]]; then
      printf 'Missing %s: set it in the environment or in .env.\n' "$key" >&2
      exit 1
    fi
  fi
  printf '%s' "$value"
}

CONSUMER_HTTP_PORT="$(require_port CONSUMER_HTTP_PORT)"
PRODUCER_HTTP_PORT="$(require_port PRODUCER_HTTP_PORT)"
readonly CONSUMER_HTTP_PORT
readonly PRODUCER_HTTP_PORT
readonly CONSUMER_READINESS_URL="http://localhost:${CONSUMER_HTTP_PORT}/actuator/health/readiness"
readonly PRODUCER_READINESS_URL="http://localhost:${PRODUCER_HTTP_PORT}/actuator/health/readiness"
readonly CONSUMER_PROMETHEUS_URL="http://localhost:${CONSUMER_HTTP_PORT}/actuator/prometheus"
readonly CONSUMER_ENV_URL="http://localhost:${CONSUMER_HTTP_PORT}/actuator/env"

print_failure_diagnostics() {
  printf '\nSmoke test failed; last 200 Compose log lines follow.\n' >&2
  compose logs --no-color --tail=200 || true
}

cleanup() {
  local status=$?
  local teardown_status=0

  trap - EXIT
  if (( status != 0 )); then
    print_failure_diagnostics
  fi

  if compose down -v; then
    :
  else
    teardown_status=$?
    if (( status == 0 )); then
      status=$teardown_status
    fi
  fi

  exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_readiness() {
  local service=$1
  local url=$2
  local deadline=$((SECONDS + READINESS_TIMEOUT_SECONDS))

  until curl --fail --silent "$url" >/dev/null; do
    if (( SECONDS >= deadline )); then
      printf 'Timed out waiting for %s readiness at %s after %ss.\n' \
        "$service" "$url" "$READINESS_TIMEOUT_SECONDS" >&2
      return 1
    fi
    sleep 2
  done

  printf '%s readiness is UP.\n' "$service"
}

find_correlated_anomaly() {
  local deadline=$((SECONDS + OBSERVATION_TIMEOUT_SECONDS))
  local consumer_logs
  local producer_logs
  local line
  local emitted_at
  local sequence
  local value
  local summary_found=false
  local anomaly_found=false
  local injection_pattern='^\[([^]]+)\] INJECTED ANOMALY seq=([0-9]+) value=(-?[0-9]+\.[0-9]{2}) \(.+\)$'

  while (( SECONDS < deadline )); do
    consumer_logs="$(compose logs --no-color --no-log-prefix --tail=1000 consumer)"
    producer_logs="$(compose logs --no-color --no-log-prefix --tail=1000 producer)"

    if [[ $consumer_logs == *'SUMMARY | processed=100'* ]]; then
      summary_found=true
    fi

    if [[ $anomaly_found != true ]]; then
      while IFS= read -r line; do
        if [[ $line =~ $injection_pattern ]]; then
          emitted_at=${BASH_REMATCH[1]}
          sequence=${BASH_REMATCH[2]}
          value=${BASH_REMATCH[3]}
          if [[ $consumer_logs == *"[$emitted_at] Data point: $value | Status: ANOMALY DETECTED!"* ]]; then
            printf 'Correlated injected anomaly: seq=%s emittedAt=%s value=%s.\n' "$sequence" "$emitted_at" "$value"
            anomaly_found=true
            break
          fi
        fi
      done <<< "$producer_logs"
    fi

    if [[ $summary_found == true && $anomaly_found == true ]]; then
      printf 'Observed the first summary at processed=100.\n'
      return 0
    fi

    sleep 2
  done

  printf 'Timed out after %ss waiting for' "$OBSERVATION_TIMEOUT_SECONDS" >&2
  if [[ $anomaly_found != true ]]; then
    printf ' an injected-anomaly correlation' >&2
  fi
  if [[ $summary_found != true ]]; then
    printf ' SUMMARY | processed=100' >&2
  fi
  printf '.\n' >&2
  return 1
}

assert_prometheus_metrics() {
  local exposition
  local metric
  local required_metrics=(
    anomaly_detector_points_processed_total
    anomaly_detector_points_anomalies_total
    anomaly_detector_window_occupancy
    anomaly_detector_processing_seconds_count
  )

  exposition="$(curl --fail --silent --show-error "$CONSUMER_PROMETHEUS_URL")"
  for metric in "${required_metrics[@]}"; do
    if [[ $exposition != *"$metric"* ]]; then
      printf 'Prometheus exposition is missing %s.\n' "$metric" >&2
      return 1
    fi
  done

  printf 'Prometheus exposition contains the required detector metrics.\n'
}

assert_forbidden_endpoint() {
  local status

  status="$(curl --silent --output /dev/null --write-out '%{http_code}' "$CONSUMER_ENV_URL")"
  if [[ $status != 404 ]]; then
    printf 'Expected %s to be unavailable (404), got HTTP %s.\n' "$CONSUMER_ENV_URL" "$status" >&2
    return 1
  fi

  printf 'Forbidden Actuator endpoint is unavailable (404).\n'
}

assert_memory_limit() {
  local service=$1
  local container
  local usage
  local percentage

  container="$(compose ps -q "$service")"
  if [[ -z $container ]]; then
    printf 'Could not resolve the %s container for memory verification.\n' "$service" >&2
    return 1
  fi

  usage="$(docker stats --no-stream --format '{{.MemUsage}}' "$container")"
  percentage="$(docker stats --no-stream --format '{{.MemPerc}}' "$container")"
  if ! awk -v percentage="${percentage%%%}" -v limit="$MEMORY_PERCENT_LIMIT" \
      'BEGIN { exit !(percentage < limit) }'; then
    printf '%s memory usage %s (%s) is not below %s%%.\n' \
      "$service" "$usage" "$percentage" "$MEMORY_PERCENT_LIMIT" >&2
    return 1
  fi

  printf '%s memory usage %s (%s) is below %s%%.\n' \
    "$service" "$usage" "$percentage" "$MEMORY_PERCENT_LIMIT"
}

# Replays the exact walkthrough printed in README section 4 against the real three-container system.
# The seeded stream depends only on the RNG draw order, never on wall-clock pacing, so a 25 ms interval
# reproduces the documented sequences ten times faster than the 250 ms default.
assert_documented_shift_trace() {
  local deadline
  local logs
  local line
  local expected=(
    'LEVEL SHIFT at seq=400: mean 100.00 -> 150.00'
    'Data point: 148.37 | Status: ANOMALY DETECTED! | Z-score: 10.84'
    'Data point: 90.35 | Status: OK | Z-score: 1.95'
    'Data point: 145.69 | Status: ANOMALY DETECTED! | Z-score: 9.92'
    'Data point: 140.24 | Status: ANOMALY DETECTED! | Z-score: 8.76'
    'Data point: 155.24 | Status: ANOMALY DETECTED! | Z-score: 11.96'
    'Data point: 152.26 | Status: ANOMALY DETECTED! | Z-score: 11.32'
    'Data point: 155.78 | Status: ANOMALY DETECTED! | Z-score: 12.07'
    'Data point: 146.39 | Status: OK | Z-score: 2.61'
  )

  printf 'Restarting with the regime-shift demo enabled.\n'
  compose down -v
  PRODUCER_LEVEL_SHIFT_ENABLED=true PRODUCER_INTERVAL_MS=25 docker compose up --build --detach
  wait_for_readiness consumer "$CONSUMER_READINESS_URL"

  deadline=$((SECONDS + OBSERVATION_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    logs="$(compose logs --no-color --no-log-prefix --tail=3000)"
    if [[ $logs == *"${expected[${#expected[@]} - 1]}"* ]]; then
      for line in "${expected[@]}"; do
        if [[ $logs != *"$line"* ]]; then
          printf 'Regime-shift trace diverged from README section 4; missing: %s\n' "$line" >&2
          return 1
        fi
      done
      printf 'Regime-shift trace matches README section 4 through the K=5 admission at seq 407.\n'
      return 0
    fi
    sleep 2
  done

  printf 'Timed out after %ss waiting for the documented regime-shift trace.\n' \
    "$OBSERVATION_TIMEOUT_SECONDS" >&2
  return 1
}

printf 'Starting a clean Compose smoke environment.\n'
compose down -v
compose up --build --detach

wait_for_readiness consumer "$CONSUMER_READINESS_URL"
wait_for_readiness producer "$PRODUCER_READINESS_URL"
find_correlated_anomaly
assert_prometheus_metrics
assert_forbidden_endpoint
assert_memory_limit consumer
assert_memory_limit producer
assert_documented_shift_trace

printf 'Smoke test passed; tearing down Compose resources.\n'
