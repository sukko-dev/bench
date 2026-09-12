# sukko-bench

Reproducible benchmark for [Sukko](https://sukko.dev): latency under an
odds-shaped burst workload, and checker-proven zero message loss while
infrastructure is killed mid-burst. Runs entirely on the **Community edition** —
no licence, on one rentable VM.

## Running it

The benchmark is driven by [Task](https://taskfile.dev), which is the only
supported entry point: a published number means these tasks, so a result is
never confounded by an ad-hoc invocation.

```sh
brew install go-task/tap/go-task     # or see taskfile.dev
task smoke                           # laptop end-to-end check
```

`task --list-all` shows every target. [REPRODUCE.md](REPRODUCE.md) is the exact
recipe for a published run (machine shape, OS limits, pinned images);
[METHODOLOGY.md](METHODOLOGY.md) explains what is measured and what is
deliberately excluded — its §8 walks a single run end to end if you would rather
start from the concrete sequence than the rules.

## Status

The Go driver, its open-loop scheduler and its zero-loss checker are complete
and unit-tested (`task test`, race-enabled). The stack boots from the released,
digest-pinned public images and a full run completes end to end.

**No number is published yet.** The remaining blocker is listed under "Open
before any published number" in [REPRODUCE.md](REPRODUCE.md); the accounting
faults that once let the checker confuse harness back-pressure with platform
loss are closed.
