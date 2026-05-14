#!/usr/bin/env bash
# Capture-corpus runner.
#
# For each scenario in tools/scenarios/*.py, start the capture proxy bound
# to 127.0.0.1:5673 forwarding to 127.0.0.1:5672, run the driver, wait
# for the proxy to exit, and verify the c2s.bin/s2c.bin pair exists.
#
# Pre-conditions:
#   - RabbitMQ 3.13 running on 127.0.0.1:5672 (docker container)
#   - tools/capture_proxy built
#   - pika installed in python3 path

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROXY="$ROOT/tools/capture_proxy"
SCENARIOS_DIR="$ROOT/tools/scenarios"
FIXTURES="$ROOT/spec/fixtures/frames"

if [[ ! -x "$PROXY" ]]; then
  echo "build proxy first: crystal build tools/capture_proxy.cr -o tools/capture_proxy" >&2
  exit 1
fi

mkdir -p "$FIXTURES"

ONLY="${1:-}"
shift || true

for script in "$SCENARIOS_DIR"/*.py; do
  name="$(basename "$script" .py)"
  [[ "$name" == _* ]] && continue
  if [[ -n "$ONLY" && "$name" != "$ONLY" ]]; then
    continue
  fi

  echo "=== $name ==="
  rm -rf "$FIXTURES/$name"

  # Start proxy in background; wait for its READY marker on stdout.
  proxy_log="$FIXTURES/$name.proxy.log"
  : > "$proxy_log"
  "$PROXY" "$name" 5673 127.0.0.1 5672 >"$proxy_log" 2>>"$proxy_log" &
  proxy_pid=$!
  for _ in $(seq 1 50); do
    if grep -q '^READY$' "$proxy_log" 2>/dev/null; then
      break
    fi
    sleep 0.05
  done

  # Run driver against the proxy.
  if ! (cd "$SCENARIOS_DIR" && python3 "$script") >"$FIXTURES/$name.driver.log" 2>&1; then
    echo "  driver failed (continuing; some scenarios elicit errors by design)"
  fi

  # Wait for proxy to finish writing.
  wait "$proxy_pid" 2>/dev/null || true

  c2s="$FIXTURES/$name/c2s.bin"
  s2c="$FIXTURES/$name/s2c.bin"
  if [[ -s "$c2s" && -s "$s2c" ]]; then
    c2s_n=$(wc -c <"$c2s" | tr -d ' ')
    s2c_n=$(wc -c <"$s2c" | tr -d ' ')
    echo "  ok: c2s=$c2s_n s2c=$s2c_n"
  else
    echo "  WARN: empty corpus for $name"
  fi
done

echo "all scenarios complete; artefacts in $FIXTURES"
