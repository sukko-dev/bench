#!/usr/bin/env bash
# Kill ONE ws-server replica mid-run. The gateway's other clients are
# unaffected; the killed replica's clients reconnect (DNS → survivor) and
# replay their gap. Prints the kill timestamp (ns) for the recovery analysis.
set -euo pipefail
project="${COMPOSE_PROJECT_NAME:-compose}"
target="$(docker ps --filter "label=com.docker.compose.service=ws-server" \
  --filter "label=com.docker.compose.project=${project}" -q | head -1)"
[ -n "$target" ] || { echo "no ws-server replica found" >&2; exit 1; }
echo "fault_kill_unixnano=$(date +%s%N)"
docker kill "$target" >/dev/null
echo "killed ws-server replica ${target:0:12}"
