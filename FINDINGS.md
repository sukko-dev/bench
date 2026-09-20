# Findings

The benchmark's job is failure: kill each infrastructure dependency mid-burst and
let the zero-loss checker decide whether the platform lost messages. Run against
the released, digest-pinned images, that matrix surfaced **three real
data-integrity bugs** in the platform — each root-caused and fixed. The fixes
ship in **v1.0.3** (the digests pinned in [`compose/docker-compose.yml`](compose/docker-compose.yml)).
This page is what the harness actually found; the latency headline is separate
and still pending the pinned-VM run (see [Status](README.md)).

## Bugs the matrix found (all fixed in v1.0.3)

| # | Bug | Exposed by | Fix |
|---|-----|-----------|-----|
| 1 | **At-least-once broken on a broadcast-bus outage** — the Kafka consumer marked offsets for commit after a *void* broadcast call, so while Valkey was down it committed past records it never delivered. Killing Redpanda lost nothing (the consumer can't advance past unread records); killing Valkey lost them permanently. | `fault-valkey` | ADR-0019 · server #6 |
| 2 | **Cross-tenant channel-isolation leak on reconnect** — reconnect replay filtered by the client's live subscription set but *skipped the filter when that set was empty*, and the protocol sends `reconnect` before `subscribe`, so it was always empty. With many channels on one topic, a reconnect replayed **every channel on the topic** — cross-tenant reachable (a client could name another tenant's channel in `last_pos`). §IX isolation violation. | `fault-ws` | ADR-0020 · server #7 |
| 3 | **Replay dropped the recovery data it was meant to deliver** — reconnect replay ran each record through the *live* consume path's rate limiter and CPU brake, silently dropping the failover burst under load (and draining the live consume budget). | `fault-ws` | ADR-0021 · server #8 |

## Fault matrix — released v1.0.3

Killed mid-burst (second burst window of `scenarios/odds-burst.toml`), then the
dependency restarted for the recovery window. Zero-loss = the checker found no
hole in any subscriber's per-channel sequence over the whole run.

| Fault (mid-burst kill) | Zero message loss | Notes |
|---|---|---|
| **Redpanda** (Kafka) | ✅ `holes=0` | consumer cannot advance past unread records; replays on recovery |
| **Valkey** (broadcast bus) | ✅ `holes=0` | at-least-once restored — bug 1 |
| **ws-server** (replica) | ✅ `misrouted=0`, and `holes=0` on **fast** failover | isolation closed (bug 2) and recovery restored (bug 3). **Open item:** when the Kafka consumer-group rebalance takes the full session-timeout (~32 s — a hard `SIGKILL` of the replica that *owns* the client's partition sends no `LeaveGroup`, so the group waits the timeout before reassigning), a residual gap remains: the client's single reconnect-replay fires immediately but the survivor doesn't serve those partitions until the stall clears, and messages published during the stall fall between replay and live-delivery. Being closed by tightening the consumer session timeout so the stall stays short. |

These are **correctness** (zero-loss) results — environment-independent, so they
reproduce on any dedicated 8-vCPU box, not only the pinned VM. Reproduce with the
recipe in [REPRODUCE.md](REPRODUCE.md): `task stack-up`, discard the first run,
then `task bench` while scheduling `task fault-<valkey|redpanda|ws>` into the
second burst window.
