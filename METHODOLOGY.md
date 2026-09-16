# Methodology

This document is the contract behind every number this repository publishes.
If a result can't be traced to the rules here, it isn't a result.

Sections 1–7 state the rules. If you would rather start concrete, **§8 walks a
single run end to end** and points back at the rule each step enforces.

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
120 channels, 4 subscribers each (480 active connections), 2 msg/s/channel
baseline (~240 msg/s aggregate), Poisson-arriving burst windows at ×8 rate for
3 s (~1,920 msg/s peak), ~300-byte JSON payloads (size-padded — payload weight
is a parameter, not an accident). Every parameter lives in the scenario TOML
and is overridable; published runs use the committed defaults.

Sizing rationale, in order of the constraints that bind:

- **The Community edition caps total connections at 500** — a license wall, not
  configuration. The headline claim is that the benchmark runs entirely on the
  free tier, so the scenario fits inside that cap (480 connections, with
  headroom for reconnect churn during the kill test). A larger-fleet throughput
  anchor would exceed the free tier and is out of scope for this artifact.
- Per-channel gap during a ~10 s outage (~20 msgs baseline, ~50 in-burst) fits
  a single replay request under the server's *default* settings
  (`WS_MAX_REPLAY_MESSAGES=100`, 10 s per-channel replay rate limit).

**Warmup.** Each scenario declares a `warmup` (smoke 1 s, odds-burst 2 s) whose
records are excluded from the latency distribution and counted in the artifact
(`warmup`, `warmup_excluded`). The **zero-loss verdict is never windowed** — a
message is not excused for being lost early.

The value is measured, not assumed. Connection establishment is already outside
the measured window by construction: the harness starts every subscriber before
the publisher, at any connection count. The only remaining transient is pipeline
cold-start, and per-second p99 across three runs localises it to the first second
(96–119 ms in second 0; 25–32 ms from second 1 on). A wall-clock warmup is
therefore a sufficient mechanism here, and an event-based "wait until every
subscriber is delivering" rule was tried and rejected: with subscribers
pre-established it computes to zero, measuring a readiness condition the harness
design already guarantees.

**Disclosed deviations from stock defaults — the two rate ceilings.** Both are
admission controls rather than pipeline capacity, and both sit below the
workload's ~1,920 msg/s peak at their stock values. The compose stack raises
each above that peak so neither shapes the measured pipeline. They are the
benchmark's only deviations from stock server configuration, both are ordinary
operator env configuration available on every edition, and a Community operator
reproduces them verbatim.

1. **Publish admission** — the gateway's default publish rate limit is 10 msg/s
   per authenticated principal (`GATEWAY_PUBLISH_RATE_LIMIT`), an anti-abuse
   control, and the whole bench publishes as one principal. Raised to 2,500
   msg/s (burst 5,000).
2. **Kafka consume ceiling** — ws-server's `WS_MAX_KAFKA_RATE` defaults to 1,000
   msg/s. Raised to 2,500 msg/s. Leaving it at the default while raising
   admission is the more dangerous of the two misconfigurations, because the two
   limits sit on *opposite ends of the same pipeline*: the publisher is admitted
   at the gateway and acknowledged, then the consumer deliberately drops the
   excess, so the loss is invisible to the publisher and appears only as holes
   confined to the burst windows. Measured on a stock-default run: 3,472 holes
   against 53,280 acknowledged publishes, with p99 latency degraded from ~15 ms
   to 1.27 s. The drops are logged and counted (`ws_kafka_messages_dropped_total`)
   — the platform is behaving exactly as configured; the configuration was wrong.

The driver additionally honours any 429's `Retry-After` on its retry path, so an
incidental rejection never turns into an immediate-retry storm.

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

## 8. Anatomy of a run

The rules above, in execution order. Each step names the invariant it exists to
hold — the harness is mostly guards against the ways a benchmark can flatter
itself, and those guards are the reason to trust the output.

### Setup

**1. Boot the stack.** Postgres (tenants, keys), Valkey (the bus between server
replicas), Redpanda (the Kafka log), the provisioning service (admin API), **two
ws-server replicas**, and the gateway — the only endpoint clients may reach.

Two replicas exist so the fault matrix (§5) can kill one mid-burst and prove
clients fail over to the survivor without losing messages. A single replica
could not demonstrate that.

**2. Provision the tenant.** Using the operator CLI, not a private path:

- **Channel rules** granting subscribe *and* publish on the workload's channels.
  Both are required; publish authorisation is separate from subscribe.
- **A routing rule** mapping channel → Kafka topic. There is no convention
  fallback: a channel with no matching rule is refused, loudly.
- **A tenant signing keypair**, registered with the platform.
- **A client token** signed by that key, which the driver presents as its identity.

> **Invariant — no private setup path.** The driver never provisions and never
> mints credentials. Everything the benchmark needs is created with the same
> tool an operator uses, so a measured result cannot depend on a capability a
> real deployment lacks.

### The measured run

**3. Connect every subscriber, then publish.** All subscribers connect,
authenticate, subscribe and receive their subscription acknowledgement *before
the first message is published*.

> **Invariant — connection setup is outside the measured window.** Establishing
> connections costs time that has nothing to do with delivery. Completing it
> before publishing removes it from the measurement structurally, at any
> connection count — not by subtracting an estimate afterwards.

**4. Publish on a pre-computed schedule.** Every message's send time is decided
before the run begins, from the scenario's baseline rate and burst windows. Each
message carries its run ID, channel, per-channel sequence number and intended
send time inside a size-padded payload.

> **Invariant — latency is `arrival − intended`, never `arrival − actual send`.**
> If the driver itself is late, that lateness counts against the result. This is
> the open-loop rule from §2, and it is what stops the harness from easing off
> precisely when the system is struggling.

**5. One message's path.** Gateway verifies the token, then checks the channel
rules permit publishing here, then checks the publish rate limit. It forwards to
a ws-server, which resolves the routing rule to a topic and writes to Kafka —
**this is the point at which a publish is *confirmed***. A ws-server reads it
back out, puts it on the Valkey bus so both replicas see it, and each replica
fans it out to its own subscribers.

Every receiving subscriber appends one line to its private receive log: arrival
time, intended time, channel, sequence number, and the platform's message ID.

### Finishing

**6. Drain, then tear down.** The publisher returns when the last message is
*confirmed*, at which point it is still being fanned out. The run waits before
closing anything.

> **Invariant — teardown never races in-flight delivery.** Without the wait, each
> channel's final message is absent from every receive log, and the report
> attributes to the platform a loss the harness caused by not waiting.

**7. Stop subscribers and read the logs.** Closing each subscriber flushes its
buffered log. Logs are only read after that — a live subscriber's log is
incomplete by construction.

### The verdict

**8. Check completeness** (§6). Per subscriber × channel, after de-duplicating
by message ID, the received sequence numbers must be exactly the published set.
A gap is a hole; an unpublished sequence is a phantom; a record on an
unsubscribed channel is misrouting. Any of the three fails the run. Redelivery
is counted and permitted — the platform promises at-least-once.

> **Invariant — loss is claimable only for acknowledged publishes.** A publish
> the platform refused was never promised to anyone, so its absence downstream
> is the harness's account, not the platform's. Rejected publishes are excluded
> from the expected set and tolerated if they arrive anyway (a lost
> acknowledgement is not a lost message).

**9. Report, behind two gates.** Latency percentiles are computed over the
receive history, excluding the scenario's warmup window.

> **Invariant — warmup windows latency only.** The completeness check always
> covers the whole run. A message is not excused for being lost early.

> **Invariant — a run fails if the harness under-delivered.** Because the loss
> verdict spans only acknowledged publishes, a run in which few were acknowledged
> proves almost nothing however clean it looks. The run therefore fails when
> nothing was confirmed, or when the unacknowledged share exceeds the threshold
> in `report.MaxUnconfirmedFraction` — reported as a *harness fault*, explicitly
> distinct from a delivery failure.

The artifact records confirmed and unacknowledged publish counts, the warmup
window and how many records it excluded, the hole count with a locating sample,
and the latency distribution. Every one of those figures is derivable from the
scenario file, so a reader can recompute what was measured and what was set
aside rather than taking the summary on trust.

Exit status: `0` passed, `1` ran but failed — a publishable outcome, since failed
runs are part of the record — `2` could not run.
