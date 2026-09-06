# Benchmark harness. `make smoke` is the laptop end-to-end check;
# `make bench SCENARIO=scenarios/odds-burst.toml` is a full run.
SHELL := /bin/bash
COMPOSE := docker compose -f compose/docker-compose.yml
REPLICAS ?= 2
SCENARIO ?= scenarios/odds-burst.toml
OUT ?= results/$(shell date -u +%Y%m%dT%H%M%SZ)

.PHONY: test build stack-up stack-down bootstrap bench smoke fault-ws fault-valkey fault-redpanda

test:
	go test -race ./...

build:
	go build -o bin/bench ./cmd/bench

stack-up:
	@command -v sukko >/dev/null || { echo "sukko CLI required (provisioning bootstrap)"; exit 1; }
	sukko auth keygen --out .bench-keys/admin.pem 2>/dev/null || true
	ADMIN_BOOTSTRAP_KEY=$$(sukko auth pubkey --key-file .bench-keys/admin.pem) \
	CREDENTIALS_ENCRYPTION_KEY=$$(openssl rand -hex 32) \
	  $(COMPOSE) up -d --build --scale ws-server=$(REPLICAS) --wait

stack-down:
	$(COMPOSE) down -v

bootstrap:
	./bootstrap.sh

bench: build bootstrap
	@set -a; source .bench.env; set +a; \
	mkdir -p $(OUT); cp $(SCENARIO) $(OUT)/scenario.toml; \
	./bin/bench --ws $$BENCH_WS --http $$BENCH_HTTP --token $$BENCH_TOKEN \
	  --scenario $(SCENARIO) --out $(OUT)

# One-command laptop check: boot, provision, run the tiny smoke scenario, tear down.
smoke:
	$(MAKE) stack-up
	$(MAKE) bench SCENARIO=scenarios/smoke.toml OUT=results/smoke
	$(MAKE) stack-down

# Fault injectors — run mid-`bench` from a second shell (or a wrapper that
# schedules them into the burst window).
fault-ws:
	COMPOSE_PROJECT_NAME=compose ./faults/kill-ws-server.sh
fault-valkey:
	COMPOSE_PROJECT_NAME=compose ./faults/kill-valkey.sh
fault-redpanda:
	COMPOSE_PROJECT_NAME=compose ./faults/kill-redpanda.sh
