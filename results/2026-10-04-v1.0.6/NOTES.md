# v1.0.6 — fault matrix (official)

- **Date:** 2026-10-04
- **Version:** v1.0.6 (released, digest-pinned)
- **Machine:** GCP `c2-standard-8` (8 dedicated vCPU, 32 GB), debian-12, zone `australia-southeast1-c`. Driver + stack co-located (one clock, loopback) per METHODOLOGY §3.
- **OS limits:** `ulimit -n 1048576`; `net.ipv4.ip_local_port_range=1024 65535`; `net.core.somaxconn=4096`.
- **Harness:** `ci/vm-run.sh MODE=release SUKKO_REF=v1.0.6 MATRIX_ARGS="--runs 5"`; scenario `odds-burst.toml`; transport `ws`. Single-node Redpanda with `write_caching=false` (fsync-before-ack) so an acked write is durable across the broker kill.

## Images (released, digest-addressed)

- `ghcr.io/sukko-dev/sukko-server@sha256:202022530d24d64d1b6466c920957b291bdc33498c925e218e8cd35b4c8b2f1a`
- `ghcr.io/sukko-dev/sukko-gateway@sha256:e6f8a148ae3c7d685a17e28d3e25ac1e2794e63cb37115ef962e36fb2896eebc`
- `ghcr.io/sukko-dev/sukko-provisioning@sha256:658603fc182648851bff43bdc1453b8d956865acdb4408a725c5ccf82dc7a0e4`

## Result

Overall PASS. **Zero message loss** across every fault class, ×5 runs each:

| Fault (×5) | holes | notes |
|---|---|---|
| clean (no fault) | 0 | p50 ≈ 25.7 ms, p99 ≈ 43 ms, p999 ≈ 48–112 ms (under the 60/120/200 ms ceilings) |
| `docker kill` Valkey | 0 | broadcast-bus outage; redelivery on return |
| `docker kill` Redpanda | 0 | ingest outage; durable acks survive the hard kill |
| `docker kill` one of two ws-server replicas | 0 | client failover + reconnect-with-replay (ADR-0026) |

Full per-run artifacts: `matrix.json` + `<fault>-<n>/result.json`.
