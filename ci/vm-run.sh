#!/usr/bin/env bash
# On-VM runner for the scheduled fault-matrix regression gate (ADR-0001).
#
# The sukko `bench-matrix` workflow rents an ephemeral GCE box, clones this repo at a pinned
# ref, and runs THIS script on it. The script installs a toolchain, resolves the sukko images
# for the requested mode, and runs `task matrix` — whose Go verdict core (internal/matrix) is
# the actual gate. The workflow does no verdict logic; it only drives the VM and reads the
# artifact this produces (ADR-0001 keeps the gate's decision in tested Go, not in workflow shell).
#
# The box is `--no-service-account --no-scopes`, so every fetch here is anonymous: sukko/bench
# clones and ghcr pulls are public, the toolchain comes from public installers. If a step ever
# needs a credential, the design is wrong — fix the resource, do not add a key.
#
# Modes:
#   main    (default) build the 3 sukko images bench boots from source at SUKKO_REF (default
#           `main`) — this is the weekly regression pass, the only way to cover a merged-to-main
#           change (merge publishes no image tag).
#   release pull the immutable :vX.Y.Z digests for the tag in SUKKO_REF — the release gate.
#
# Environment (the workflow sets these; all have defaults for a hand-run):
#   MODE          main | release                      (default: main)
#   SUKKO_REF     main mode: git ref to build;         (default: main)
#                 release mode: the vX.Y.Z tag (used for BOTH the checkout and the image tag)
#   GO_VERSION    Go toolchain to install              (default: 1.26.0)
#   CLI_VERSION   sukko CLI release to install         (default: 1.0.2)
#   SCENARIO      scenario TOML for the matrix         (default: scenarios/odds-burst.toml)
#   MATRIX_ARGS   extra flags for cmd/matrix           (default: none — full faults × default runs)
#   OUT           output dir (holds matrix.json + per-run result.json) (default: matrix-out)
#
# Exit status is `task matrix`'s: non-zero on a reproduced regression. The workflow scp's $OUT
# back and treats a non-zero exit as the gate failing.

set -euo pipefail

# The VM's benchmark work needs the docker socket and writes system paths; run the whole thing
# as root on this single-purpose ephemeral box rather than juggling the docker group on a
# non-login SSH shell (usermod does not take effect for the current session). -E keeps the MODE/
# SUKKO_REF/... env across the privilege change.
if [ "$(id -u)" -ne 0 ]; then
  exec sudo -E "$0" "$@"
fi

MODE="${MODE:-main}"
SUKKO_REF="${SUKKO_REF:-main}"
GO_VERSION="${GO_VERSION:-1.26.0}"
CLI_VERSION="${CLI_VERSION:-1.0.2}"
SCENARIO="${SCENARIO:-scenarios/odds-burst.toml}"
MATRIX_ARGS="${MATRIX_ARGS:-}"
OUT="${OUT:-matrix-out}"

# Run from the bench repo root (this script lives in ci/).
cd "$(dirname "$0")/.."
BENCH_DIR="$PWD"

log() { printf '\n\033[1m[vm-run] %s\033[0m\n' "$*"; }

# ── Toolchain (all anonymous) ────────────────────────────────────────────────
log "installing toolchain (docker, go ${GO_VERSION}, task, sukko ${CLI_VERSION})"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git curl ca-certificates openssl >/dev/null

if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sh >/dev/null 2>&1
fi

if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "go${GO_VERSION}"; then
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tgz
  rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/go.tgz
fi
export PATH="/usr/local/go/bin:/usr/local/bin:${PATH}"

if ! command -v task >/dev/null; then
  curl -fsSL https://taskfile.dev/install.sh | sh -s -- -d -b /usr/local/bin >/dev/null 2>&1
fi

if ! command -v sukko >/dev/null; then
  curl -fsSL "https://github.com/sukko-dev/cli/releases/download/v${CLI_VERSION}/sukko_${CLI_VERSION}_linux_amd64.tar.gz" -o /tmp/sukko.tgz
  tar -C /usr/local/bin -xzf /tmp/sukko.tgz sukko
fi

log "versions"
docker --version
go version
task --version
sukko version

# ── Resolve the sukko images per mode ────────────────────────────────────────
case "$MODE" in
  main)
    # Build the 3 images bench boots (server, gateway, provisioning) from source. VERSION/
    # COMMIT_HASH/BUILD_TIME mirror the release build-args so the binaries self-identify.
    log "MODE=main: building sukko images from source at ${SUKKO_REF}"
    rm -rf /tmp/sukko-src
    git clone -q "https://github.com/sukko-dev/sukko.git" /tmp/sukko-src
    git -C /tmp/sukko-src checkout -q "$SUKKO_REF"
    SHA="$(git -C /tmp/sukko-src rev-parse --short HEAD)"
    for svc in server gateway provisioning; do
      log "  build $svc ($SHA)"
      docker build -q \
        -f "/tmp/sukko-src/ws/build/${svc}/Dockerfile" \
        --build-arg VERSION="ci-${SHA}" \
        --build-arg COMMIT_HASH="${SHA}" \
        --build-arg BUILD_TIME="$(date -u +%FT%TZ)" \
        -t "sukko-local/${svc}:ci" /tmp/sukko-src/ws
    done
    export SUKKO_IMAGE_SERVER="sukko-local/server:ci"
    export SUKKO_IMAGE_GATEWAY="sukko-local/gateway:ci"
    export SUKKO_IMAGE_PROVISIONING="sukko-local/provisioning:ci"
    ;;
  release)
    # Gate the immutable released artifact: pull the :vX.Y.Z tag (public on ghcr).
    [ -n "$SUKKO_REF" ] || { echo "MODE=release requires SUKKO_REF=<vX.Y.Z tag>" >&2; exit 2; }
    log "MODE=release: pulling immutable images for ${SUKKO_REF}"
    export SUKKO_IMAGE_SERVER="ghcr.io/sukko-dev/sukko-server:${SUKKO_REF}"
    export SUKKO_IMAGE_GATEWAY="ghcr.io/sukko-dev/sukko-gateway:${SUKKO_REF}"
    export SUKKO_IMAGE_PROVISIONING="ghcr.io/sukko-dev/sukko-provisioning:${SUKKO_REF}"
    ;;
  *)
    echo "unknown MODE '$MODE' (want main|release)" >&2
    exit 2
    ;;
esac
log "images: server=$SUKKO_IMAGE_SERVER gateway=$SUKKO_IMAGE_GATEWAY provisioning=$SUKKO_IMAGE_PROVISIONING"

# ── Run the matrix ───────────────────────────────────────────────────────────
# `task matrix` boots the stack on those images, provisions the tenant, runs each fault × N
# with the deterministic burst-window kill, aggregates to $OUT/matrix.json, and (via defer)
# tears the stack down. Its exit status is the gate.
log "running the fault matrix (scenario=$SCENARIO out=$OUT args=[$MATRIX_ARGS])"
cd "$BENCH_DIR"
# Capture the gate's exit rather than letting set -e abort here, so the artifact path is always
# logged and the status is propagated deliberately (a reproduced regression is a non-zero exit,
# not an error to swallow).
status=0
task matrix SCENARIO="$SCENARIO" OUT="$OUT" MATRIX_ARGS="$MATRIX_ARGS" || status=$?

log "matrix complete (exit $status); artifacts in $OUT"
exit "$status"
