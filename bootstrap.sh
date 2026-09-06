#!/usr/bin/env bash
# Provision a bench tenant and mint a token, then write the runtime env the
# driver needs to ./.bench.env. Uses the operator `sukko` CLI (the tool built
# for exactly this) against the compose-booted provisioning + gateway — the
# driver itself never provisions or mints (ADR-0012).
#
# Prereqs: `sukko` on PATH; the stack up (make stack-up); admin key material
# already injected into the provisioning service (make stack-up sets
# ADMIN_BOOTSTRAP_KEY / CREDENTIALS_ENCRYPTION_KEY from `sukko auth keygen`).
set -euo pipefail

PROV_URL="${PROV_URL:-http://localhost:18080}"
GW_WS="${GW_WS:-ws://localhost:3000}"
GW_HTTP="${GW_HTTP:-http://localhost:3000}"
TENANT="${TENANT:-bench}"
KEYDIR="${KEYDIR:-.bench-keys}"
mkdir -p "$KEYDIR"

# 1. Admin keypair + register with provisioning (idempotent).
[ -f "$KEYDIR/admin.pem" ] || sukko auth keygen --out "$KEYDIR/admin.pem"
sukko auth register --api "$PROV_URL" --key-file "$KEYDIR/admin.pem" || true

# 2. Tenant (idempotent — a 409 ALREADY_EXISTS is fine).
sukko tenant create --api "$PROV_URL" --slug "$TENANT" --name "Bench" || true

# 3. Tenant JWT signing keypair + register the public half.
[ -f "$KEYDIR/tenant.pem" ] || sukko key generate --out "$KEYDIR/tenant.pem"
sukko key register --api "$PROV_URL" --tenant "$TENANT" --key-file "$KEYDIR/tenant.pem" || true

# 4. Mint a client token signed by the tenant private key (ttl covers the run).
TOKEN="$(sukko token generate --tenant "$TENANT" --sub bench-driver \
  --key-file "$KEYDIR/tenant.pem" --algorithm EdDSA --ttl 1h)"

cat > .bench.env <<ENV
BENCH_WS=$GW_WS
BENCH_HTTP=$GW_HTTP
BENCH_TOKEN=$TOKEN
BENCH_TENANT=$TENANT
ENV
echo "wrote .bench.env (tenant=$TENANT)"
# NOTE (unsmoked): exact `sukko` subcommand flags above are drawn from the CLI
# surface; validate against a real boot on first run and correct any drift.
