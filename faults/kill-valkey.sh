#!/usr/bin/env bash
# Kill the Valkey broadcast bus mid-run: fan-out stalls until it returns;
# consumer offsets redeliver. Restart it after the outage window.
set -euo pipefail
project="${COMPOSE_PROJECT_NAME:-compose}"
target="$(docker ps --filter "label=com.docker.compose.service=valkey" \
  --filter "label=com.docker.compose.project=${project}" -q | head -1)"
[ -n "$target" ] || { echo "no valkey container found" >&2; exit 1; }
echo "fault_kill_unixnano=$(date +%s%N)"
docker kill "$target" >/dev/null
echo "killed valkey ${target:0:12}"
