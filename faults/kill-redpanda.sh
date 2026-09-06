#!/usr/bin/env bash
# Kill Redpanda mid-run: ingest pauses; producers buffer/retry, and on return
# the backlog drains. Restart it after the outage window.
set -euo pipefail
project="${COMPOSE_PROJECT_NAME:-compose}"
target="$(docker ps --filter "label=com.docker.compose.service=redpanda" \
  --filter "label=com.docker.compose.project=${project}" -q | head -1)"
[ -n "$target" ] || { echo "no redpanda container found" >&2; exit 1; }
echo "fault_kill_unixnano=$(date +%s%N)"
docker kill "$target" >/dev/null
echo "killed redpanda ${target:0:12}"
