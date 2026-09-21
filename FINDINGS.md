# Findings

The benchmark's job is failure: kill each infrastructure dependency mid-burst and
let the zero-loss checker decide whether the platform lost messages. Run against
the released, digest-pinned images, that matrix surfaced **four real
data-integrity bugs** in the platform — each root-caused and fixed. The fixes
ship in **v1.0.3** and **v1.0.4** (the digests pinned in [`compose/docker-compose.yml`](compose/docker-compose.yml)).
This page is what the harness actually found — the zero-loss fault matrix below,
and the [latency headline](#latency--released-v104) under burst.

## Bugs the matrix found (fixed in v1.0.3 / v1.0.4)

| # | Bug | Exposed by | Fix |
|---|-----|-----------|-----|
| 1 | **At-least-once broken on a broadcast-bus outage** — the Kafka consumer marked offsets for commit after a *void* broadcast call, so while Valkey was down it committed past records it never delivered. Killing Redpanda lost nothing (the consumer can't advance past unread records); killing Valkey lost them permanently. | `fault-valkey` | ADR-0019 · server #6 |
| 2 | **Cross-tenant channel-isolation leak on reconnect** — reconnect replay filtered by the client's live subscription set but *skipped the filter when that set was empty*, and the protocol sends `reconnect` before `subscribe`, so it was always empty. With many channels on one topic, a reconnect replayed **every channel on the topic** — cross-tenant reachable (a client could name another tenant's channel in `last_pos`). §IX isolation violation. | `fault-ws` | ADR-0020 · server #7 |
| 3 | **Replay dropped the recovery data it was meant to deliver** — reconnect replay ran each record through the *live* consume path's rate limiter and CPU brake, silently dropping the failover burst under load (and draining the live consume budget). | `fault-ws` | ADR-0021 · server #8 |
| 4 | **Consume-loop rate limiter dropped-and-committed-past over-rate records** — when a partition is reassigned after an owner `SIGKILL`, the survivor drains the accumulated backlog faster than `WS_MAX_KAFKA_RATE`; the limiter shed the excess *and marked it committed*, permanently losing it (~700 holes on a deterministic owner-kill). Now paces (bounded-blocks) instead of dropping. Lowering the session timeout was tried and rejected — it barely moved the loss. | `fault-ws` | ADR-0022 · server #9 (v1.0.4) |

## Fault matrix — released v1.0.4

Killed mid-burst (second burst window of `scenarios/odds-burst.toml`), then the
dependency restarted for the recovery window. Zero-loss = the checker found no
hole in any subscriber's per-channel sequence over the whole run.

| Fault (mid-burst kill) | Zero message loss | Notes |
|---|---|---|
| **Redpanda** (Kafka) | ✅ `holes=0` | consumer cannot advance past unread records; replays on recovery |
| **Valkey** (broadcast bus) | ✅ `holes=0` | at-least-once restored — bug 1 |
| **ws-server** (replica) | ✅ `misrouted=0`, `holes=0` — incl. the slow rebalance | isolation closed (bug 2), recovery restored (bug 3), and the slow-rebalance residual closed (bug 4). Verified with a **deterministic owner-kill** (kill the replica that owns the client's partition, so the group waits the full ~32 s session-timeout before reassigning): `holes=0` across `clean`, `fault-valkey`, `fault-redpanda`, and two owner-kill runs on the released v1.0.4 images. The residual was the consume-loop rate limiter dropping the reassignment catch-up backlog (bug 4), not the session-timeout — lowering the timeout was tried and did not close it. |

These are **correctness** (zero-loss) results — environment-independent, so they
reproduce on any dedicated 8-vCPU box, not only the pinned VM. Reproduce with the
recipe in [REPRODUCE.md](REPRODUCE.md): `task stack-up`, discard the first run,
then `task bench` while scheduling `task fault-<valkey|redpanda|ws>` into the
second burst window.

## Latency — released v1.0.4

End-to-end delivery latency (publish → client arrival) under the odds-shaped
burst workload (`scenarios/odds-burst.toml`: 120 channels × 4 subscribers,
~240 msg/s baseline with ×8 bursts), no fault. Measured **open-loop** (Gil Tene /
wrk2 style — each message's intended send time is fixed by the schedule, so a
slow send lands in the recorded latency instead of a shifted clock; see
[METHODOLOGY.md](METHODOLOGY.md) §2). Warm: the first run after boot is discarded
(pipeline cold-start), then three consecutive warm runs. Each run is 211,200
per-delivery samples across 480 subscribers, `negative=0` (no pre-arrival skew).

| warm run | p50 | p99 | p999 | max | driver CPU |
|---|---|---|---|---|---|
| 1 | 28.5 ms | 49.9 ms | 56.4 ms | 61.2 ms | 13.5 % |
| 2 | 28.7 ms | 51.6 ms | 57.0 ms | 62.4 ms | 13.5 % |
| 3 | 28.8 ms | 50.8 ms | 56.5 ms | 61.1 ms | 13.5 % |

**~28 ms median, ~57 ms p999 — under ×8 burst, on the released v1.0.4 images.**
The driver held ~13.5 % of the box's CPU across each run, so these measure the
platform, not the load generator (METHODOLOGY §2, driver honesty — the
`driver_cpu` field is in every `result.json`).

**Disclosures.** Single pinned, dedicated-CPU VM (GCP `c2-standard-8`, 8 vCPU);
driver and stack are co-located, so results **exclude WAN latency** — a real
deployment adds its RTT on top of every number here (METHODOLOGY §3). The
first-run (cold) tail is much higher (a one-off pipeline warm-up); discarding it
is part of the recipe. Reproduce: `task stack-up`, discard the first
`task bench SCENARIO=scenarios/odds-burst.toml`, then three more warm.
