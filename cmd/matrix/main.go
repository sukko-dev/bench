// Command matrix runs the fault matrix as a regression gate (ADR-0001).
//
// For each fault it runs the bench scenario N times, deterministically firing the fault into the
// second burst window and restarting the killed dependency, then aggregates the runs into a
// pass/fail verdict (internal/matrix) and writes matrix.json. Exits non-zero on a reproduced
// regression. The stack must already be up — `task matrix` boots it, runs this, and tears it down.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sukko-dev/bench/internal/config"
	"github.com/sukko-dev/bench/internal/matrix"
	"github.com/sukko-dev/bench/internal/report"
)

func main() {
	scenario := flag.String("scenario", "scenarios/odds-burst.toml", "scenario TOML")
	faultsCSV := flag.String("faults", "clean,valkey,redpanda,ws-server", "comma-separated faults; 'clean' = no fault")
	runs := flag.Int("runs", 3, "runs per fault")
	out := flag.String("out", "matrix-results", "output dir for per-run artifacts + matrix.json")
	ws := flag.String("ws", os.Getenv("BENCH_WS"), "gateway WS base URL")
	httpURL := flag.String("http", os.Getenv("BENCH_HTTP"), "gateway HTTP base URL")
	token := flag.String("token", os.Getenv("BENCH_TOKEN"), "tenant JWT")
	benchBin := flag.String("bench", "./bin/bench", "path to the bench binary")
	faultsDir := flag.String("faults-dir", "faults", "directory of kill-<fault>.sh scripts")
	composeFile := flag.String("compose-file", "compose/docker-compose.yml", "compose file for restarting killed deps")
	project := flag.String("project", "compose", "compose project name")
	p50 := flag.Duration("p50", 60*time.Millisecond, "p50 latency ceiling (0 disables)")
	p99 := flag.Duration("p99", 120*time.Millisecond, "p99 latency ceiling (0 disables)")
	p999 := flag.Duration("p999", 200*time.Millisecond, "p999 latency ceiling (0 disables)")
	offset := flag.Duration("fault-offset", time.Second, "how far into the second burst to fire the fault")
	outage := flag.Duration("outage", 5*time.Second, "how long the dependency stays down before restart")
	flag.Parse()

	if *ws == "" || *httpURL == "" || *token == "" {
		fatal(fmt.Errorf("--ws, --http, --token (or BENCH_WS/BENCH_HTTP/BENCH_TOKEN) are required"))
	}
	// A zero (or negative) run count would make matrix.Run iterate no runs per fault, leave every
	// fault's OK true, and exit 0 — a matrix that ran nothing yet reports all-PASS. Reject it here.
	if *runs < 1 {
		fatal(fmt.Errorf("--runs must be >= 1, got %d", *runs))
	}

	cfg, err := loadScenario(*scenario)
	if err != nil {
		fatal(err)
	}
	if len(cfg.Bursts) == 0 {
		fatal(fmt.Errorf("scenario %s has no bursts — cannot time the fault into a burst window", *scenario))
	}
	// The fault fires into the LAST (second) burst window — where the published load is heaviest
	// and the recovery path is most stressed.
	lastBurst := cfg.Bursts[len(cfg.Bursts)-1]
	burstStart := time.Duration(float64(cfg.Duration) * lastBurst.StartFraction)
	delay := matrix.FaultDelay(cfg.Duration, lastBurst.StartFraction, *offset)
	// A --fault-offset larger than the burst is worse than useless: the kill fires after the burst
	// (or after the whole run ends), so the fault never meets the measured load and the run can pass
	// with the failure mode untested. Reject a mistimed offset rather than shipping a green that lied.
	if !matrix.FaultLandsInBurst(delay, burstStart, lastBurst.Duration) {
		fatal(fmt.Errorf("fault would fire at +%s, outside the last burst window [%s, %s) of a %s run; "+
			"choose --fault-offset in [0, %s)", delay, burstStart, burstStart+lastBurst.Duration, cfg.Duration, lastBurst.Duration))
	}

	// Create the output root up front so writeReport can always land matrix.json, even on a
	// configuration where no per-run subdir was created (e.g. every fault erroring before its run).
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal(err)
	}

	d := &runner{
		bench: *benchBin, ws: *ws, http: *httpURL, token: *token, scenario: *scenario,
		outRoot: *out, faultsDir: *faultsDir, composeFile: *composeFile, project: *project,
		faultDelay: delay, outage: *outage,
	}

	rep := matrix.Run(strings.Split(*faultsCSV, ","), *runs, matrix.Thresholds{P50: *p50, P99: *p99, P999: *p999}, d.run)

	if err := writeReport(*out, rep); err != nil {
		fatal(err)
	}
	printSummary(rep, delay)
	if !rep.OK {
		os.Exit(1)
	}
}

// runner holds the orchestration config; run is the matrix.RunFunc.
type runner struct {
	bench, ws, http, token, scenario         string
	outRoot, faultsDir, composeFile, project string
	faultDelay, outage                       time.Duration
}

// run executes one bench run under fault (run index runIdx) and returns its result. For a fault it
// starts bench, fires the kill into the burst window, restores the dependency after the outage,
// then reads result.json. A kill/restart failure is a run error (retried by matrix.Run).
func (r *runner) run(fault string, runIdx int) (report.Result, error) {
	outDir := filepath.Join(r.outRoot, fmt.Sprintf("%s-%d", fault, runIdx))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return report.Result{}, err
	}
	cmd := exec.Command(r.bench, "--ws", r.ws, "--http", r.http, "--token", r.token,
		"--scenario", r.scenario, "--out", outDir)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr

	if fault == "clean" {
		if err := cmd.Run(); !benchCompleted(err) {
			return report.Result{}, fmt.Errorf("bench clean run: %w", err)
		}
	} else {
		if err := cmd.Start(); err != nil {
			return report.Result{}, fmt.Errorf("start bench: %w", err)
		}
		time.Sleep(r.faultDelay)
		if err := r.script(filepath.Join(r.faultsDir, "kill-"+fault+".sh")); err != nil {
			_ = cmd.Wait()
			return report.Result{}, fmt.Errorf("inject fault %s: %w", fault, err)
		}
		time.Sleep(r.outage)
		if err := r.compose("start", fault); err != nil { // restore the killed dependency
			_ = cmd.Wait()
			return report.Result{}, fmt.Errorf("restart %s: %w", fault, err)
		}
		if err := cmd.Wait(); !benchCompleted(err) {
			return report.Result{}, fmt.Errorf("bench %s run: %w", fault, err)
		}
	}
	// The run completed (exit 0 or 1) so bench wrote result.json; let the verdict layer judge it.
	// A FAILED run (exit 1, holes/latency) MUST reach Classify with its real numbers — treating it
	// as a bare exec error would blank the matrix's evidence for exactly the runs that regressed.
	return readResult(filepath.Join(outDir, "result.json"))
}

// benchCompleted reports whether a bench process exit means result.json was written. Per cmd/bench's
// exit contract, 0 (pass) and 1 (completed-but-failed) both write the artifact before exiting; 2
// (could not execute), a signal, or a start failure mean there is no artifact to read. So a nil
// error or an *exec.ExitError with code 1 is "completed"; anything else is a run error.
func benchCompleted(err error) bool {
	if err == nil {
		return true
	}
	var ee *exec.ExitError
	return errors.As(err, &ee) && ee.ExitCode() == 1
}

func (r *runner) script(path string) error {
	cmd := exec.Command("bash", path)
	cmd.Env = append(os.Environ(), "COMPOSE_PROJECT_NAME="+r.project)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func (r *runner) compose(args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose", "-f", r.composeFile, "-p", r.project}, args...)...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func loadScenario(path string) (config.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return config.Config{}, err
	}
	defer func() { _ = f.Close() }()
	return config.Parse(f)
}

func readResult(path string) (report.Result, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return report.Result{}, err
	}
	var res report.Result
	if err := json.Unmarshal(b, &res); err != nil {
		return report.Result{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return res, nil
}

func writeReport(outRoot string, rep matrix.Report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outRoot, "matrix.json"), b, 0o644)
}

func printSummary(rep matrix.Report, delay time.Duration) {
	fmt.Printf("\n=== fault matrix (fault fired at +%s) ===\n", delay)
	for _, f := range rep.Faults {
		status := "PASS"
		if !f.OK {
			status = "FAIL"
		}
		retried := ""
		if f.Retried {
			retried = " (retried)"
		}
		fmt.Printf("  %-10s %s%s\n", f.Fault, status, retried)
		for _, run := range f.Runs {
			if !run.OK {
				fmt.Printf("      × holes=%d p99=%s: %s\n", run.Holes, run.P99, strings.Join(run.Reasons, "; "))
			}
		}
	}
	if rep.OK {
		fmt.Println("=== VERDICT: PASS — zero data loss, latency within ceilings ===")
	} else {
		fmt.Println("=== VERDICT: FAIL — reproduced regression (see above) ===")
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "matrix:", err)
	os.Exit(2)
}
