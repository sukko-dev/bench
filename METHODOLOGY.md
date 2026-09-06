# Methodology

This document is the contract behind every number this repository publishes.
If a result can't be traced to the rules here, it isn't a result.

## 1. Claims — and non-claims

Three claims, nothing else:

- **C1 — Latency under burst.** End-to-end delivery latency (publish → client
  arrival) distribution for an odds-shaped workload, measured open-loop on a
  pinned, rentable VM.
- **C2 — Zero loss under kill.** Killing a ws-server replica, the Valkey
  broadcast bus, or the Redpanda broker mid-burst loses zero messages — proven
  by a checker over the full receive history, not asserted.
- **C3 — Bounded recovery.** Time-to-reconnect and time-to-gap-closed
  distributions per fault, with the recovery boundary disclosed: gaps
  exceeding the replay window surface as explicit `PossibleGap` signals,
  never silent loss.

Non-claims, deliberately:

- **No connection-count headline.** A millions-of-connections test is a
  memory-and-idle-keepalive test — connections sit; almost nothing flows per
  connection. It measures a different thing, requires a distributed load rig
  (multiple generator hosts, multiple source IPs, cross-host time sync), and
  is a possible follow-up artifact on a multi-node topology — not this one.
  This benchmark keeps every connection *active* and measures delivery.
- **No competitor numbers.** The harness is generic enough that others can
  point it at other systems; we publish only our own.
- **No WAN latency.** See §3 — we measure what the software adds, not the
  internet.

## 2. Coordinated-omission safety

The publisher is open-loop (Gil Tene / wrk2): every message's *intended* send
time is fixed by the schedule alone (`internal/schedule` — pure arithmetic
over a piecewise-constant rate profile). A slow send makes the publisher late
relative to the schedule; the lateness lands in the recorded latency
(`arrival − intended`), never in a silently shifted schedule. Latency is
recorded in HDR histograms; percentiles are reported only where the sample
count supports them (≥5,000 samples for a p99).

## 3. Why one VM (and what that trades away)

Driver and stack are co-located on a single pinned, dedicated-CPU VM:

- **One clock.** Publish and arrival timestamps come from the same monotonic
  clock. At single-digit-millisecond claims, cross-host clock error would
  dominate the measurement; loopback eliminates it.
- **Loopback, not NIC.** Traffic never leaves the machine, so NIC bandwidth
  and packet-rate ceilings vanish and the bottleneck is the platform's CPU
  work per message — the thing being measured.
- **The disclosed cost:** results exclude network latency. A real deployment
  adds its WAN RTT on top of every number here. We are measuring what the
  software contributes to latency, not what the internet does.
- **Driver honesty.** Driver CPU time is measured and reported per run so the
  load generator can't silently be the bottleneck (or the excuse).

OS limits are tuned and disclosed in REPRODUCE.md (`ulimit -n`, ephemeral
port range, `somaxconn`) — a stranger's reproduction must not stall on
`too many open files`.

## 4. Workload — "odds-burst"

Many channels × low per-channel rate, the shape of market data:
500 channels, 4 subscribers each (2,000 active connections), 2 msg/s/channel
baseline (~1k msg/s aggregate), Poisson-arriving burst windows at ×8 rate for
3 s, ~300-byte JSON payloads (size-padded — payload weight is a parameter,
not an accident). A separate throughput anchor runs the same shape at 10k
connections. Every parameter lives in the scenario TOML and is overridable;
published runs use the committed defaults.

Sizing rationale: per-channel gap during a ~10 s outage (~20 msgs baseline,
~50 in-burst) fits a single replay request under the server's *default*
settings (`WS_MAX_REPLAY_MESSAGES=100`, 10 s per-channel replay rate limit).
Server defaults are not tuned for the benchmark; the workload is sized to the
defaults and says so.

## 5. Fault matrix

Mid-burst, one fault per run, ×5 runs each:

| Fault | What it exercises |
|---|---|
| `docker kill` one of two ws-server replicas | client failover + reconnect-with-replay |
| `docker kill` Valkey | broadcast-bus outage; consumer-offset redelivery on return |
| `docker kill` Redpanda | ingest outage; producer buffering / backfill on return |

Recovery metrics per fault: fault→reconnected, reconnected→gap-closed,
checker verdict.

## 6. The checker

Zero loss is a verdict computed from data (`internal/check`), per
subscriber × subscribed channel, after deduplicating deliveries by `mid`
(the platform's stable message identity):

- any missing seq — including a missing *tail* — is a hole → **FAIL**;
- a seq never published is a phantom → **FAIL** (it would mean our own
  accounting is untrustworthy);
- a delivery on a channel the subscriber never subscribed to is misrouted →
  **FAIL**;
- redelivered mids (at-least-once transport) and same-seq-under-new-mid
  (publisher retry after ambiguous failure) are **counted and published,
  never failing** — that is the disclosed delivery contract: at-least-once,
  client-side dedupe by `mid`.

Failed runs are published with their artifacts. The checker's falsifiability
is the credibility.

## 7. Rigor rules

- Pinned VM shape (exact provider slug in REPRODUCE.md), images pinned by
  digest to a released version.
- 60 s warmup excluded; ≥5 runs per scenario; median and spread reported.
- Raw HDR logs and receive-log-derived results committed per run under
  `results/<date>-<version>/`.
- Everything runs on the Community edition.
