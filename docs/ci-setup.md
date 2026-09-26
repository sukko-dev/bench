# Scheduled fault-matrix regression gate — full task, step by step

The fault matrix (kill Valkey / Redpanda / a ws-server shard owner mid-burst and assert zero
data loss, plus the ×8-burst latency headline) is what caught the cycle-1 and cycle-2 broadcast
data loss — the whole v1.0.4 arc. Today it only runs when a human manually kicks it. This task
turns it into a scheduled regression gate: a GitHub Actions workflow in **`sukko-dev/sukko`**
that rents an ephemeral GCE box, runs this repo's harness on it, and fails on a *reproduced*
regression. Design and decisions are in `docs/adr/0001-scheduled-fault-matrix-regression-guard.md`.

This document is the ordered task list — every step, finished and unfinished. Finished steps
carry a **Verify** block: the exact command and the output that proves it. All Verify commands
below were re-run green against the live project/repo on 2026-09-26.

**Shell prerequisites for the Verify/Do commands:**

```bash
gcloud config set account red@sukko.dev      # owner on sukko-bench
gcloud config set project sukko-bench
gh auth switch --user klurvio                 # the account with write on sukko-dev/*
# gh's "active account" is global state other sessions flip. A read-only account returns
# HTTP 403 on variable reads, which looks like "variable missing" — always confirm:
gh api user --jq .login                        # must print: klurvio
```

Fixed identifiers this task uses:

| | |
|---|---|
| GCP project | `sukko-bench` (number `950453418090`) |
| Zone / region | `australia-southeast1-b` / `australia-southeast1` |
| Service account | `bench-matrix@sukko-bench.iam.gserviceaccount.com` |
| WIF pool / provider | `github-actions` / `sukko-github` |
| Repo the gate lives in | `sukko-dev/sukko` |
| Harness repo (this) | `sukko-dev/bench` |

---

## Part A — Design & verdict core  ✅ DONE

### Step 1 — Record the decision (ADR-0001)  ✅

The two-layer design (committed Go runner here; the gate *workflow* in `sukko`), dispatch-first
rollout, N=3, `holes==0` hard gate + generous absolute latency ceilings, retry-once-then-alert,
GCE self-destruct.

**Verify:**
```bash
gh pr view 7 --repo sukko-dev/bench --json state,title,files \
  --jq '.state, .title, (.files[].path | select(contains("adr")))'
# → OPEN
#   feat: matrix verdict core + regression-guard ADR
#   docs/adr/0001-scheduled-fault-matrix-regression-guard.md
```

### Step 2 — Build the verdict core (`internal/matrix`)  ✅

The pure verdict/aggregation/retry gate: `Classify` reuses the authoritative `report.Result.Pass`
(zero-loss + harness-sound + recovery-ok) and adds latency ceilings; `Run` does N-runs-per-fault
+ retry-once with the retry batch authoritative. Mutation-proven — dropping the holes check, the
retry, or a latency ceiling each fails a test.

**Verify:**
```bash
export GOROOT=$HOME/.gvm/gos/go1.26.2 GOTOOLCHAIN=auto
export PATH=$GOROOT/bin:$HOME/.gvm/pkgsets/go1.26.2/global/bin:$PATH
cd <bench worktree> && go test -race ./internal/matrix
# → ok  github.com/sukko-dev/bench/internal/matrix
```

Steps 1–2 are on branch `feat/scheduled-regression-guard`, open as **bench PR #7**.

---

## Part B — GCP prerequisite (keyless auth)  ✅ DONE

Workflow authenticates with Workload Identity Federation — no long-lived key. Each step below
is a one-time `gcloud`/`gh` command that was already run; the **Verify** proves the resulting
state. `create` verbs are not idempotent (they fail `ALREADY_EXISTS`), so re-running the *Do*
lines is unnecessary — trust the Verify.

> Never "start clean" by deleting the pool or provider: deletion **soft-deletes for 30 days**
> and the name cannot be reused until purged or undeleted. If something is wrong, converge it
> (e.g. `providers update-oidc`), don't recreate it.

### Step 3 — Dedicated project + billing  ✅

A separate project (not `sukko-prod`) is a blast-radius decision, not a cost one — projects are
free, and this keeps a CI-assumable "create/delete VMs" credential out of production.

*Done with:* `gcloud projects create sukko-bench --name="Sukko bench CI"` then
`gcloud billing projects link sukko-bench --billing-account=<acct>`.

**Verify:**
```bash
gcloud billing projects describe sukko-bench --format='value(billingEnabled)'
# → True
```

### Step 4 — Enable the five APIs  ✅

WIF needs `iam` + `iamcredentials` + `sts` + `cloudresourcemanager`; the gate drives `compute`.
The `sts` endpoint is the token exchange itself — omit it and the workflow's first auth step
fails with an opaque error. (The original setup enabled only three; `sts` and
`cloudresourcemanager` were added later — this was the real gap.)

*Done with:* `gcloud services enable compute.googleapis.com iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com cloudresourcemanager.googleapis.com --project sukko-bench`

**Verify** — must print exactly 5 lines (do **not** `head` the raw list; it is alphabetical and
`sts` sorts near the end, so truncation hides a missing `sts`):
```bash
gcloud services list --enabled --project sukko-bench --format='value(config.name)' \
  | grep -E '^(compute|iam|iamcredentials|sts|cloudresourcemanager)\.googleapis\.com$' | sort
```

### Step 5 — Service account + role  ✅

`compute.instanceAdmin.v1` is the whole job: create, read, delete one VM and write its SSH
metadata.

*Done with:* `gcloud iam service-accounts create bench-matrix …` then
`gcloud projects add-iam-policy-binding sukko-bench --member="serviceAccount:bench-matrix@sukko-bench.iam.gserviceaccount.com" --role="roles/compute.instanceAdmin.v1"`

**Verify:**
```bash
gcloud projects get-iam-policy sukko-bench --flatten='bindings[].members' \
  --filter="bindings.members:bench-matrix@sukko-bench.iam.gserviceaccount.com" \
  --format='value(bindings.role)'
# → roles/compute.instanceAdmin.v1
#   roles/iam.serviceAccountUser      ← see the cleanup note at the end; safe to drop
```

### Step 6 — WIF pool + OIDC provider  ✅

The provider trusts GitHub. The **attribute condition is the security boundary**: only workflows
in `sukko-dev/sukko` can exchange a token.

*Done with:* `workload-identity-pools create github-actions …` then
`workload-identity-pools providers create-oidc sukko-github … --issuer-uri="https://token.actions.githubusercontent.com" --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" --attribute-condition="assertion.repository=='sukko-dev/sukko'"`

**Verify:**
```bash
gcloud iam workload-identity-pools providers describe sukko-github \
  --project sukko-bench --location=global --workload-identity-pool=github-actions \
  --format='yaml(state,oidc.issuerUri,attributeMapping,attributeCondition)'
# → state: ACTIVE
#   oidc.issuerUri: https://token.actions.githubusercontent.com
#   attributeMapping: {attribute.repository: assertion.repository, google.subject: assertion.sub}
#   attributeCondition: assertion.repository=='sukko-dev/sukko'
```

### Step 7 — Let only this repo impersonate the SA  ✅

`principalSet` + `attribute.repository` = "any workflow run in this repository", scoped by the
provider condition above. This is the binding that makes it keyless.

*Done with:* `gcloud iam service-accounts add-iam-policy-binding bench-matrix@… --role="roles/iam.workloadIdentityUser" --member="principalSet://…/attribute.repository/sukko-dev/sukko"`
(failed once on a new-project IAM propagation race, succeeded on retry.)

**Verify:**
```bash
gcloud iam service-accounts get-iam-policy bench-matrix@sukko-bench.iam.gserviceaccount.com \
  --project sukko-bench --flatten='bindings[].members' \
  --filter="bindings.role:roles/iam.workloadIdentityUser" --format='value(bindings.members)'
# → principalSet://iam.googleapis.com/projects/950453418090/locations/global/workloadIdentityPools/github-actions/attribute.repository/sukko-dev/sukko
```

### Step 8 — Four GitHub repository variables  ✅

Variables, not secrets — none is sensitive, and readable values make a failed run diagnosable.
`GCP_WIF_PROVIDER` is keyed by project **number**, not id.

*Done with:* `gh variable set GCP_PROJECT|GCP_ZONE|GCP_SERVICE_ACCOUNT|GCP_WIF_PROVIDER --repo sukko-dev/sukko --body …`

**Verify:**
```bash
gh variable list --repo sukko-dev/sukko | grep '^GCP_'
# → GCP_PROJECT          sukko-bench
#   GCP_SERVICE_ACCOUNT  bench-matrix@sukko-bench.iam.gserviceaccount.com
#   GCP_WIF_PROVIDER     projects/950453418090/locations/global/workloadIdentityPools/github-actions/providers/sukko-github
#   GCP_ZONE             australia-southeast1-b
```

### Step 9 — Confirm the two host assumptions  ✅

**Verify** — OS Login must be off (else the SA also needs `roles/compute.osAdminLogin` to SSH),
and there must be C2 headroom for exactly one `c2-standard-8`:
```bash
gcloud compute project-info describe --project sukko-bench \
  --format='value(commonInstanceMetadata.items)'
# → (empty)   ← OS Login not enforced, metadata-key SSH works

gcloud compute regions describe australia-southeast1 --project sukko-bench \
  --flatten='quotas[]' --format='value(quotas.metric,quotas.limit)' | awk '$1=="C2_CPUS"'
# → C2_CPUS  8.0
# (note: `regions describe` accepts --flatten but NOT --filter, hence awk)
```
`C2_CPUS=8` = one box, zero headroom → the workflow must serialize (`concurrency`, no cancel).

---

## Part C — The matrix runner (slice 2b)  🟡 BUILT, NOT COMMITTED

### Step 10 — `cmd/matrix` + `task matrix`  ✅ built, ⬜ not shipped

The runner: boots the stack once, fires each fault deterministically into the burst-2 window
(replacing the human-in-a-second-shell), restarts the killed dep, reads each `result.json`,
emits `matrix.json`, exits non-zero on a reproduced regression. Plus `FaultDelay` (burst-2 timing)
and a `Latency` JSON round-trip, both unit-tested.

**Verify (that it builds and tests pass):**
```bash
export GOROOT=$HOME/.gvm/gos/go1.26.2 GOTOOLCHAIN=auto
export PATH=$GOROOT/bin:$HOME/.gvm/pkgsets/go1.26.2/global/bin:$PATH
cd <bench worktree>
go build ./... && go test -race ./internal/matrix ./internal/report
# → both: ok
```

> Honest boundary: the verdict/timing/round-trip logic is unit-tested; the docker/bench/fault
> *plumbing* can only be exercised against a live stack. That is what the first `workflow_dispatch`
> run on the VM validates end-to-end — the point of dispatch-first.

### Step 11 — Commit slice 2b and open/extend the PR  ⬜ UNFINISHED

Stage explicit paths (the worktree also holds `docs/ci-setup.md` — keep them separate):
```
cmd/matrix/  Taskfile.yml  internal/matrix/matrix.go  internal/matrix/faultdelay_test.go
internal/report/report.go  internal/report/latency_roundtrip_test.go
```
**Open decision:** fold into PR #7 (recommended — #7 is unmerged, one coherent "runner" PR) or
ship a separate stacked PR. bench repo has no CI, so it merges on local verification (Step 10).

**Done when:** `gh pr view 7 --repo sukko-dev/bench --json files` lists `cmd/matrix/main.go`.

---

## Part D — The gate workflow (slice 3)  🟡 v1 DRAFTED, NOT MERGED

### Step 12 — v1: dispatch-only auth proof  ✅ drafted, ⬜ not merged

`.github/workflows/bench-matrix.yml` (untracked in worktree `trees/bench-gate`, branch
`feat/bench-matrix-gate`): `workflow_dispatch` only, `id-token: write`, `concurrency: bench-matrix`
non-cancelling, `auth@v3` + `setup-gcloud@v3` SHA-pinned; one job authenticates keyless and runs
`gcloud compute instances list`. `actionlint` clean.

**Land it** — `main` is protected (required checks: Commit provenance, Lint, Test, Helm Lint,
Compose Transform Tests, e2e guard fixtures), so it goes via PR:
```bash
# in a sukko worktree on a branch off origin/main
git add .github/workflows/bench-matrix.yml && git commit && git push -u origin HEAD
gh pr create --repo sukko-dev/sukko --fill      # merge --squash after checks are green
```
**`workflow_dispatch` does not exist until the file is on `main`** — this is the one step that
cannot be validated from a branch, and it blocks Steps 13–17.

**Done when:** `gh workflow list --repo sukko-dev/sukko | grep -i 'Bench Fault Matrix'`.

### Step 13 — Dispatch v1 and confirm the chain  ⬜ UNFINISHED

```bash
gh workflow run bench-matrix.yml --repo sukko-dev/sukko --ref main
gh run watch --repo sukko-dev/sukko \
  "$(gh run list --repo sukko-dev/sukko --workflow=bench-matrix.yml -L1 --json databaseId --jq '.[0].databaseId')"
```
**Done when:** the *Verify identity* step's log shows `gcloud auth list` printing
`bench-matrix@sukko-bench.iam.gserviceaccount.com` and `instances list` exits 0 (`Listed 0 items.`
is the correct result). That one line proves OIDC → STS → impersonation → Compute.

*After this, Steps 14–15 iterate from the branch — `gh workflow run --ref <branch>` dispatches the
file as it exists on that branch, no merge per attempt.*

### Step 14 — v2: ephemeral VM lifecycle  ⬜ UNFINISHED

Create the `c2-standard-8` with `--max-run-duration` + `--instance-termination-action=DELETE`
(GCE-native self-destruct) and `--no-service-account --no-scopes`; SSH smoke; delete in
`always()`. **Done when:** a dispatch creates a VM, SSHes in, and the box is gone afterward
(`gcloud compute instances list` empty).

### Step 15 — v3: the real matrix run  ⬜ UNFINISHED (needs Step 11 merged)

On the VM: install Docker + Go, check out **bench at a pinned ref** (must contain `cmd/matrix`),
then build `sukko@main` from source (weekly path) or pull the immutable `:vX.Y.Z` digests (release
path); `task matrix`; pull `matrix.json` back; the runner's exit code is the gate.

**Open decision — how the workflow reaches the VM / retrieves `matrix.json`:**
- **Public-IP SSH (recommended)** — works today, zero extra setup (`default-allow-ssh` from
  `0.0.0.0/0` exists, OS Login off, `instanceAdmin` writes SSH metadata); `gcloud compute scp`
  returns the artifact.
- IAP tunnel — lets the VM drop its public IP but adds `iap.googleapis.com` +
  `roles/iap.tunnelResourceAccessor` + firewall `35.235.240.0/20`.
- GCS hand-off bucket — extra bucket + storage role for what `scp` already does; drop.

---

## Part E — Alerting (slice 4)  ⬜ UNFINISHED

### Step 16 — Create the de-dup label  ⬜
```bash
gh label create bench-regression --repo sukko-dev/sukko \
  --description "Fault-matrix gate reproduced a broadcast-loss or latency regression" --color B60205
```
(Issues are already enabled: `gh api repos/sukko-dev/sukko --jq .has_issues` → `true`.)

### Step 17 — Judge + issue alert  ⬜

On a reproduced regression the job fails **and** creates/updates a de-duped `bench-regression`
issue (matrix table + artifact link); a green run auto-closes it. The job needs `issues: write`
(repo default is read-only).

---

## Part F — Turn the schedule on  ⬜ UNFINISHED (do last)

### Step 18 — Add `schedule` (weekly) + `release` triggers  ⬜

ADR-0001 is deliberate: a gate that has never gone green must not be scheduled. Add these triggers
only after a manual run (Steps 13–17) has passed end to end.

---

## Least-privilege cleanup (after Step 14 works)

The VM is created `--no-service-account`, so the SA's `roles/iam.serviceAccountUser` (from Step 5)
is unnecessary and over-broad — at project scope it lets the CI identity act as *any* SA in the
project, including the Compute default. Once VM creation is confirmed working:
```bash
gcloud projects remove-iam-policy-binding sukko-bench \
  --member="serviceAccount:bench-matrix@sukko-bench.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```
**Verify:** re-run Step 5's check — only `roles/compute.instanceAdmin.v1` should remain — then
re-dispatch and confirm VM creation still succeeds.

---

## WIF failure → cause (for Steps 13–15)

Error strings are reconstructions; if a real dispatch fails with different wording, trust the
live error.

| Symptom | Cause |
|---|---|
| no token minted / `Credentials file … not found` | `permissions: id-token: write` missing from workflow or job |
| `Unable to acquire impersonated credentials` | Step 7 binding missing or its `principalSet` repo slug wrong |
| `The audience in ID Token … does not match` | `allowedAudiences` set on the provider but the auth action sends its default |
| `attribute condition … denied` / `Permission denied on resource` | Step 6 condition doesn't match the dispatching repo |
| `API [sts.googleapis.com] not enabled` / opaque 403 on exchange | Step 4 incomplete — re-run its Verify |
| `Required 'compute.instances.list' permission` | Step 5 role binding missing |
