package main

import (
	"os/exec"
	"testing"
)

// TestBenchCompleted pins the exit-code contract the runner relies on: bench writes result.json
// on exit 0 (pass) and exit 1 (completed-but-failed), so both must be "completed" and read back;
// exit 2 (could not execute) and any other failure must be a run error. Real ExitErrors are used
// because *exec.ExitError's ProcessState cannot be constructed directly.
func TestBenchCompleted(t *testing.T) {
	run := func(script string) error { return exec.Command("bash", "-c", script).Run() }

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"exit 0 (pass) → completed", run("exit 0"), true},
		{"exit 1 (failed, artifact written) → completed", run("exit 1"), true},
		{"exit 2 (could not execute) → run error", run("exit 2"), false},
		{"exit 3 → run error", run("exit 3"), false},
		{"binary not found → run error", exec.Command("no-such-bench-binary-xyz").Run(), false},
	}
	for _, c := range cases {
		if got := benchCompleted(c.err); got != c.want {
			t.Errorf("%s: benchCompleted(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}
