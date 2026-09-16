# Reproducing the numbers

Every published result comes from **one pinned, rentable VM** running the
compose stack and the driver together. This document is the exact recipe.

## The machine

- **Shape:** a dedicated-CPU 8-vCPU / 16 GB cloud VM. Reference slug:
  DigitalOcean `c-8` (Premium Intel), ~$0.50/hr. Any equivalent dedicated
  (not shared/burstable) 8-vCPU box reproduces the shape; burstable CPUs do
  not — they throttle mid-burst and pollute the tail.
- **Why one box:** driver and stack share it, so publish and arrival
  timestamps come from one clock (no cross-host skew at ms-scale claims) and
  traffic is loopback (no NIC ceiling). The disclosed cost: results exclude
  WAN latency — they measure what the software adds. See METHODOLOGY.md §3.

## OS limits (set before the run)

The driver holds thousands of sockets; the defaults will stall a large run
with `too many open files` and blame the wrong thing.

```sh
ulimit -n 1048576                                  # fds: 2k conns ≈ 4k+ fds both ends
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"   # ephemeral source ports
sudo sysctl -w net.core.somaxconn=4096             # connect-ramp backlog
```

## Toolchain

- Docker + compose v2.
- Go 1.26+ (to build the driver).
- The `sukko` CLI on PATH (provisions the bench tenant + mints the token;
  the driver never provisions).
- [Task](https://taskfile.dev) — the task runner every Sukko repo uses, and the
  only supported entry point here (`brew install go-task/tap/go-task`, or see
  taskfile.dev for other platforms).

## Run it

```sh
task stack-up                       # boot: postgres, valkey, redpanda,
                                    # provisioning, 2× ws-server, gateway

# DISCARD the first run after stack-up — see below. Its output is not a result.
task bench SCENARIO=scenarios/odds-burst.toml OUT=/tmp/discard-warmup

task bench SCENARIO=scenarios/odds-burst.toml OUT=results/$(date -u +%Y%m%dT%H%M%SZ)
```

**The first run after `stack-up` MUST be discarded.** The scenario's `warmup`
covers pipeline cold-start, not *container* cold-start, and a freshly booted
stack is materially slower. Measured on the reference machine, same commit and
same configuration:

| | p50 | p99 | p999 | max |
|---|---|---|---|---|
| first run after `stack-up` | 27.8 ms | **879 ms** | 2.81 s | 2.82 s |
| warm runs (×3, consecutive) | 27.9 ms | **49.4 ms** | 54.3 ms | 58.0 ms |

A 17× difference in p99 with nothing else changed. The warm runs agree with each
other to within 0.8% on p99, so once the stack is warm the measurement is stable
and a single run is representative; the cold run is simply not a measurement of
the software. Publishing one would understate the platform by more than an order
of magnitude, and a reproducer who runs the recipe once and stops would conclude
the opposite of what the artifact claims.

`task bench` provisions (bootstrap.sh), builds the driver, runs the scenario,
and writes `result.json` + raw per-subscriber receive logs under `OUT/`.

For the failure runs, schedule a fault into the second burst window from a
second shell while `bench` is running:

```sh
sleep <into-the-burst> && task fault-ws        # or fault-valkey / fault-redpanda
```

Then restart the killed dependency (`docker compose ... start <svc>`) for the
recovery-window measurement.

## Laptop smoke (not a published number)

```sh
task smoke
```

Boots the stack, runs a 30 s / 10-channel scenario, tears down — proves the
harness end to end without the pinned machine.

## Publishing official numbers

The compose stack pins the **released, digest-addressed v1.0.1 images**
(ADR-0012) — `ghcr.io/sukko-dev/sukko-{server,gateway,provisioning}` by SHA256
digest, overridable via `SUKKO_IMAGE_*` for a later release. Commit `OUT/`
under `results/<date>-<version>/`, and record the machine slug + `sysctl`
values in the run's notes.

## Status

The Go driver and its analysis are unit-tested and `-race`-clean.

**First live boot completed 2026-09-11** (source-built images, unlicensed Community
stack). What it established:

- The harness runs end to end: provision → subscribe → publish → fan-out → receive
  → checker, with healthy latency (p50 ~16 ms, p99 ~33 ms on a laptop).
- `bootstrap.sh` drift is corrected and the corrections are recorded in the script.
- **Two blocking stack-config bugs were found and fixed** in `compose/docker-compose.yml`:
  the gateway had no `PROVISIONING_GRPC_ADDR`, so its key/API-key/revocation streams
  never connected and every token failed as unverifiable.

**Open before any published number:** nothing — the harness defects are closed, and
the compose stack is pinned to `v1.0.1`, the first release carrying the
routing-rules and Retry-After changes. What remains before official numbers is
operational: rent the pinned VM and run the fault matrix.

**Closed** (2026-09-12): the publisher/checker accounting fault, the vacuous-pass
fault, and the inert warmup flag — `warmup` now lives in the scenario TOML (excluded
from the latency distribution only; the zero-loss check always covers the whole run,
and `result.json` reports `warmup`/`warmup_excluded`). The working checker then
immediately caught a fourth defect: a teardown race losing each channel's FINAL seq
(publish confirmed, fan-out still in flight when subscribers were stopped) — closed
with a 2s drain grace between publisher completion and teardown, pinned by an
ordering test. `result.json` also now embeds `holes_sample` evidence so a failing
run can be triaged from the artifact alone. Verified: three consecutive smokes,
holes=0, all confirmed publishes delivered. The retry path now honours 429 `Retry-After` (with capped exponential backoff
when unhinted); the checker claims loss only for ACKNOWLEDGED publishes — unconfirmed
seqs are excluded from the coverage requirement but tolerated if they arrive — and
`result.json` reports `confirmed_publishes`/`unconfirmed_publishes`. A run FAILS as a
harness fault when nothing was confirmed or the unconfirmed share exceeds 1%
(`report.HarnessSound`), so the shrunken-denominator and zero-delivery greens are both
impossible. Publish admission is raised above the workload peak in the compose stack —
the disclosed deviation in METHODOLOGY.md §4. The `odds-burst` scenario is sized inside
the Community 500-connection cap (120×4 = 480).

## Publishing official numbers

The compose stack pins the **released, digest-addressed v1.0.1 images**
(ADR-0012) — `ghcr.io/sukko-dev/sukko-{server,gateway,provisioning}` by SHA256
digest, overridable via `SUKKO_IMAGE_*` for a later release. Commit `OUT/`
under `results/<date>-<version>/`, and record the machine slug + `sysctl`
values in the run's notes.

## Status

The Go driver and its analysis are unit-tested and `-race`-clean.

**First live boot completed 2026-09-11** (source-built images, unlicensed Community
stack). What it established:

- The harness runs end to end: provision → subscribe → publish → fan-out → receive
  → checker, with healthy latency (p50 ~16 ms, p99 ~33 ms on a laptop).
- `bootstrap.sh` drift is corrected and the corrections are recorded in the script.
- **Two blocking stack-config bugs were found and fixed** in `compose/docker-compose.yml`:
  the gateway had no `PROVISIONING_GRPC_ADDR`, so its key/API-key/revocation streams
  never connected and every token failed as unverifiable.

**Open before any published number:**

1. **The driver outruns the gateway's publish rate limit, and miscounts the result.**
   Root-caused 2026-09-11 from `gateway_rest_publish_total`: of ~18,300 attempts only
   **796 succeeded**; ~10,900 were `rate_limited` (429) and the rest `forbidden`
   (pre-setup). The two smoke runs delivered 792 + 794 = 1586 receives against
   796 accepted publishes × 2 subscribers per channel = 1592 expected — i.e. the
   platform delivered essentially **everything it accepted**. There is no delivery
   loss. Two fixes are needed on the harness side:
   - Pace the publisher to the gateway's per-tenant publish limit (or raise the limit
     deliberately for the bench tenant and disclose it in METHODOLOGY).
   - **Stop counting rejected publishes as expected deliveries.** `pub.Run` sets
     `m.Published[ch] = seq` BEFORE dispatch, so it records sequence numbers *issued*;
     sends that never confirm land in `m.Unconfirmed` and the checker never consults it.
     `check.Run` then requires every subscriber to have received `1..Published[ch]`,
     turning each rejected publish into a Hole for every subscriber. Either exclude
     `Unconfirmed` from the coverage requirement, or only advance `Published` on
     confirmation.

2. **Vacuous-pass bug in the checker.** A run that delivered ZERO messages reported
   `pass=true, holes=0`. Zero delivered must fail — otherwise a totally broken run
   ships as a green result.

3. **The `--warmup` flag is inert.** `cmd/bench/main.go` parses it and discards it
   (`_ = *warmup`); the "analysis step" that would apply the window does not exist.
   Either implement the windowing or remove the flag — as it stands it silently
   promises an exclusion that never happens.

Note for (1): a Hole is a *contiguous range* of missing seqs per (subscriber, channel),
not one missing message — so the headline "454 holes" counted gap-ranges produced by a
single systemic cause, not 454 independent faults.
