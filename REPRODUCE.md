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

## Run it

```sh
make stack-up                       # boot: postgres, valkey, redpanda,
                                    # provisioning, 2× ws-server, gateway
make bench SCENARIO=scenarios/odds-burst.toml OUT=results/$(date -u +%Y%m%dT%H%M%SZ)
```

`make bench` provisions (bootstrap.sh), builds the driver, runs the scenario,
and writes `result.json` + raw per-subscriber receive logs under `OUT/`.

For the failure runs, schedule a fault into the second burst window from a
second shell while `bench` is running:

```sh
sleep <into-the-burst> && make fault-ws        # or fault-valkey / fault-redpanda
```

Then restart the killed dependency (`docker compose ... start <svc>`) for the
recovery-window measurement.

## Laptop smoke (not a published number)

```sh
make smoke
```

Boots the stack, runs a 30 s / 10-channel scenario, tears down — proves the
harness end to end without the pinned machine.

## Publishing official numbers

The compose stack pins the **released, digest-addressed v1.0.0 images**
(ADR-0012) — `ghcr.io/sukko-dev/sukko-{server,gateway,provisioning}` by SHA256
digest, overridable via `SUKKO_IMAGE_*` for a later release. Commit `OUT/`
under `results/<date>-<version>/`, and record the machine slug + `sysctl`
values in the run's notes.

## Status

The Go driver and its analysis are unit-tested and `-race`-clean. The compose
stack now pulls the released public images (no source build). Pending a first
booted-stack smoke: `bootstrap.sh` (provisions the bench tenant + token via the
released `sukko` CLI) and the 2-replica DNS-round-robin failover behaviour must
be confirmed against a live boot before the first published run.
