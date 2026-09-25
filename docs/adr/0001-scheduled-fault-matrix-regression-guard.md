# ADR-0001: Scheduled fault-matrix + latency regression guard

**Status**: Accepted
**Date**: 2026-09-25

## Context

The fault matrix (kill Valkey / Redpanda / a ws-server shard-owner mid-burst and assert
zero data loss) plus the ×8-burst latency headline is what caught the cycle-1 and cycle-2
broadcast data loss — the entire v1.0.4 arc. Today it only runs when a human manually kicks
it on the GCP `sukko-bench` VM, and the fault injection is a person in a second shell
(`faults/kill-*.sh`); the deterministic owner-kill harness used during v1.0.4 was ad-hoc and
never committed. So the protection that caught real data loss is not institutionalized — a
regression merged to `main` between manual runs goes unnoticed.

## Decision

Automate the fault matrix + latency headline as a scheduled regression guard, in two layers
with a clean seam:

**Layer A — a committed matrix runner** (`cmd/matrix`, Go): boots the stack once, then for
each fault × N runs, starts a `bench` run and **schedules the fault deterministically into
the second burst window** (replacing the human-in-a-second-shell), restarts the killed
dependency, collects each `result.json`, and emits an aggregate `matrix.json` + summary. It
is runnable on any box (`task matrix`); CI is just one caller. The run-loop and verdict live
in Go so they are unit-tested and mutation-verifiable — a gate that lies (a vacuous green) is
worse than no gate, so the verdict logic must not be an untestable shell script.

**Layer B — a GitHub Actions workflow that lives in the `sukko` repo** (NOT here): it checks
out this repo's harness at a pinned ref, authenticates to GCP via **Workload Identity
Federation** (keyless, no stored key), creates an ephemeral **c2-standard-8** VM, runs Layer A
over SSH, pulls `matrix.json` back as an artifact, judges it, alerts, and tears the VM down.
The workflow lives in `sukko` because that is where the code it gates lives: the `release`
trigger fires natively where releases happen (no cross-repo dispatch bridge), the main-branch
trigger watches `sukko`'s own main, and a sukko developer sees the regression gate in sukko's
Actions. The harness stays a separate, pinned, reusable tool here (ADR-0012's reproducibility
separateness is deliberate — the harness is not folded into sukko).

**Rollout**: the sukko workflow ships `workflow_dispatch`-only first — a gate that has never
gone green must not be scheduled, and it cannot run at all until the GCP WIF pool + service
account exist. Once a manual run is green end-to-end, a deliberate follow-up adds `schedule`
(weekly) + `release`. Each state is unambiguous (manual = validating; scheduled = live).

**Keystone choices:**
- **Images**: weekly builds `sukko@main` from source on the VM (true main coverage — the
  v1.0.4 loss was a main-branch regression pre-release); a release run pulls that release's
  immutable `:vX.Y.Z` digests (an immutable release gate). Merge-to-main publishes no image
  tag, so building on the VM is how main is covered without a moving `:main` tag.
- **N = 3 runs/fault**: the loss is intermittent, so N=1 is unsound; the fault
  deterministically triggers the failure mode when the bug is present, and the retry pass
  adds confirmation, so 3 triangulates without the published headline's ×5 wall-clock.
- **Gate**: `holes == 0` across all runs is the hard fail. Latency (p50/p99/p999 under ×8)
  fails only on a gross regression against **generous** absolute ceilings (p50<60 / p99<120
  / p999<200 ms, well above the ~28.5/50/56.5 headline) so tail noise never flakes it; exact
  numbers are recorded every run regardless. A breach or a hole triggers **retry-once**; only
  a reproduced failure is a regression (separating a real regression from an infra flake).
- **Alert**: on a reproduced regression the sukko workflow job fails **and** creates/updates a de-duped
  `bench-regression` GitHub issue (matrix table + artifact link); a green run auto-closes it.
- **Cost safety**: teardown runs in `always()`, and the VM is created with GCE-native
  `--max-run-duration` + `--instance-termination-action=DELETE` so it self-destructs even if
  the workflow process vanishes entirely — no separate reaper to maintain.

## Consequences

- A `main` regression in the delivery pipeline surfaces within a week (and every release is
  gated) instead of waiting for a human to remember to run the matrix.
- The matrix runner becomes a first-class, tested artifact — the fault timing is
  deterministic and reproducible, not a person's stopwatch.
- Weekly cost is a few dozen minutes of one ~$0.35/hr dedicated VM; negligible.
- A GCP Workload Identity Federation pool + a scoped service account must exist (one-time
  setup); the workflow holds no long-lived key.
- Latency gating is deliberately loose at first; ceilings can tighten once the VM's
  run-to-run variance is known from accumulated runs.

## Alternatives rejected

- **A shell matrix wrapper** instead of `cmd/matrix` — the verdict/aggregation/fault-timing
  logic is exactly where a bug makes the gate silently pass; it must be unit-testable, which a
  shell script is not. Go also matches the repo's TDD idiom.
- **Test released digests only (no main build)** — simpler, but misses regressions merged to
  main between releases, i.e. exactly the v1.0.4 class (a pre-release main-branch loss).
- **A separate scheduled VM reaper** for cost safety — more machinery than the GCE-native
  self-destruct backstop, which already guarantees deletion if the workflow dies.
- **Relative-to-baseline latency gating** — precise but needs a durable baseline store and is
  itself noisy early; revisit if generous absolute ceilings prove inadequate.
- **GCP-native scheduling (Cloud Scheduler)** or a **self-hosted ephemeral runner** — keep
  triggers/logs/history away from where the rest of CI lives, or add runner-lifecycle and
  VM-leak ownership; GH Actions + `gcloud` + WIF keeps it in one place with keyless auth.
