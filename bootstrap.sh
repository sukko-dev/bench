#!/usr/bin/env bash
# Provision a bench tenant and mint a token, then write the runtime env the
# driver needs to ./.bench.env. Uses the operator `sukko` CLI (the tool built
# for exactly this) against the compose-booted provisioning + gateway — the
# driver itself never provisions or mints (ADR-0012).
#
# Prereqs: `sukko` on PATH; the stack up (task stack-up); admin key material
# already injected into the provisioning service (task stack-up sets
# ADMIN_BOOTSTRAP_KEY / CREDENTIALS_ENCRYPTION_KEY from `sukko auth keygen`).
#
# VALIDATED against a live boot 2026-09-11 (first real smoke). Drift corrected
# in that pass, recorded here so it is not re-introduced:
#   - `sukko keys create --generate` replaces the old `key generate`/`key register`
#     pair (one command generates, registers, and saves the private half).
#   - the gateway serves WebSocket at /ws — BENCH_WS must carry the path.
#   - channel rules must grant BOTH subscribe (`public`) and publish
#     (`publish_public`), or REST publish returns 403 FORBIDDEN.
#   - a routing rule is required on the kafka backend, and is available on every
#     edition since ADR-0014 (Community is capped at 10 rules, not gated).
#   - the gateway requires BOTH `kid` and `jti` on a tenant JWT; `sukko token generate`
#     emits both (the kid header was added in cli fix/token-generate-kid-header).
#   - admin key material is the STACK-UP's job, not this script's: `sukko auth keygen`
#     takes no flags, writes to the active context dir, and refuses to overwrite. The
#     stack must already be booted with ADMIN_BOOTSTRAP_KEY set to that public key, so
#     this script only needs tenant-level provisioning.
#   - the provisioning URL flag is `--api-url` (global), not `--api`.
set -euo pipefail

PROV_URL="${PROV_URL:-http://localhost:18080}"
GW_WS="${GW_WS:-ws://localhost:3000/ws}"
GW_HTTP="${GW_HTTP:-http://localhost:3000}"
TENANT="${TENANT:-bench}"
KEYDIR="${KEYDIR:-.bench-keys}"
mkdir -p "$KEYDIR"

# 1. Tenant (idempotent — a 409 ALREADY_EXISTS is fine). Admin auth comes from the
# active context's keypair, which the booted stack already trusts via ADMIN_BOOTSTRAP_KEY.
sukko tenant create --api-url "$PROV_URL" --slug "$TENANT" --name "Bench" || true

# 2. Channel rules — the driver subscribes to and publishes on "<prefix>-N"
# channels (config.go builds "<tenant>.<prefix>-<i>"). BOTH lists are required:
# `public` authorises subscribe, `publish_public` authorises REST publish.
CHRULES="$KEYDIR/channel-rules.json"
cat > "$CHRULES" <<'JSON'
{
  "public": ["md-*"],
  "publish_public": ["md-*"]
}
JSON
sukko rules channels set --api-url "$PROV_URL" --tenant "$TENANT" --file "$CHRULES"

# 3. Routing rule — on the kafka backend a publish with no matching rule is
# rejected 409 PUBLISH_NOT_ROUTABLE (#179 removed the convention fallback).
# Ungated on every edition since ADR-0014; Community's cap is 10 rules.
sukko rules routing add --api-url "$PROV_URL" --tenant "$TENANT" \
  --pattern "**" --topics default --priority 1 || true

# 4. Tenant JWT signing keypair — generates, registers the public half, and
# saves the private half under the CLI key store keyed by --key-id.
sukko keys create --api-url "$PROV_URL" --tenant "$TENANT" --generate \
  --key-id benchkey --algorithm ES256 || true

# 5. Mint a client token signed by the tenant private key (ttl covers the run).
# The JWT carries the `kid` header the gateway needs to resolve the signing key;
# with no --key-id it is derived from the key-file basename, which is the id
# `keys create --key-id` registered.
KEYPEM="${KEYPEM:-$HOME/Library/Application Support/sukko/keys/$TENANT/benchkey.pem}"
TOKEN="$(sukko token generate --tenant "$TENANT" --sub bench-driver \
  --key-file "$KEYPEM" --key-id benchkey --algorithm ES256 --ttl 1h)"

cat > .bench.env <<ENV
BENCH_WS=$GW_WS
BENCH_HTTP=$GW_HTTP
BENCH_TOKEN=$TOKEN
BENCH_TENANT=$TENANT
ENV
echo "wrote .bench.env (tenant=$TENANT)"
