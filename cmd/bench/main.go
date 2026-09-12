// Command bench runs one benchmark scenario against a live Sukko gateway and
// writes the result artifact. Provisioning and token minting are the harness's
// job (see bootstrap.sh / the Taskfile) — this binary takes a gateway URL and
// a ready token as inputs.
//
// Usage:
//
//	bench --ws ws://host:3000 --http http://host:3000 --token <jwt> \
//	      --scenario scenarios/odds-burst.toml --out results/<dir> [--run-id ID]
//
// Exit codes: 0 the run completed and both the zero-loss checker and the
// harness-health guard PASSED; 1 the run completed but FAILED (loss/phantom/
// misrouting, or a harness fault — a published result); 2 the run could not
// execute. Warmup is a scenario parameter (TOML `warmup`), not a flag: it
// windows the latency distribution only, never the loss check.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sukko-dev/bench/internal/config"
	"github.com/sukko-dev/bench/internal/pub"
	"github.com/sukko-dev/bench/internal/report"
	"github.com/sukko-dev/bench/internal/restpub"
	"github.com/sukko-dev/bench/internal/rlog"
	"github.com/sukko-dev/bench/internal/scenario"
	"github.com/sukko-dev/bench/internal/sub"
)

// holesSampleMax caps the hole evidence embedded in result.json.
const holesSampleMax = 10

func main() {
	os.Exit(run())
}

func run() int {
	wsURL := flag.String("ws", "", "gateway WebSocket base URL (ws:// or wss://)")
	httpURL := flag.String("http", "", "gateway HTTP base URL for REST publish")
	token := flag.String("token", "", "tenant JWT (minted by the harness)")
	scenarioPath := flag.String("scenario", "", "path to the scenario TOML")
	outDir := flag.String("out", "", "directory for result artifacts (created if absent)")
	runID := flag.String("run-id", "", "run identifier; defaults to the scenario basename + start time")
	flag.Parse()

	if *wsURL == "" || *httpURL == "" || *token == "" || *scenarioPath == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "bench: --ws, --http, --token, --scenario, and --out are all required")
		return 2
	}

	f, err := os.Open(*scenarioPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: open scenario: %v\n", err)
		return 2
	}
	cfg, err := config.Parse(f)
	f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		return 2
	}

	id := *runID
	if id == "" {
		base := filepath.Base(*scenarioPath)
		id = base[:len(base)-len(filepath.Ext(base))]
	}
	logsDir := filepath.Join(*outDir, "rlogs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "bench: create out dir: %v\n", err)
		return 2
	}

	// One shared T0: the scenario's clock and the publisher's clock must agree, or
	// the warmup cutoff (and intended-time latency math) would carry a skew between
	// the two time.Now() calls that used to sit in each.
	t0 := time.Now()

	deps := scenario.Deps{
		StartSub: func(ctx context.Context, subID string, channels []string) (scenario.Subscriber, error) {
			return sub.Start(ctx, sub.Config{
				URL:      *wsURL,
				Token:    *token,
				RunID:    id,
				ClientID: subID,
				Channels: channels,
				LogPath:  filepath.Join(logsDir, subID+".rlog"),
			})
		},
		RunPub: func(ctx context.Context, plans []pub.ChannelPlan) (pub.Manifest, error) {
			publisher := restpub.New(*httpURL, *token)
			return pub.Run(ctx, pub.Config{
				RunID:       id,
				T0:          t0,
				Duration:    cfg.Duration,
				PayloadSize: cfg.PayloadSize,
				Plans:       plans,
				Send:        publisher.Send,
				SleepUntil:  sleepUntil,
			})
		},
	}

	res, err := scenario.Run(context.Background(), scenario.Config{
		RunID:          id,
		URL:            *wsURL,
		Token:          *token,
		Channels:       cfg.Channels,
		SubsPerChannel: cfg.SubsPerChannel,
		BaselineRate:   cfg.BaselineRate,
		Bursts:         cfg.Bursts,
		Duration:       cfg.Duration,
		PayloadSize:    cfg.PayloadSize,
		T0:             t0,
	}, deps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: run failed: %v\n", err)
		return 2
	}

	// Latency over every subscriber's received records, minus the warmup window
	// (the zero-loss check below is NOT windowed).
	var all []rlog.Record
	entries, _ := os.ReadDir(logsDir)
	for _, e := range entries {
		recs, rerr := rlog.ReadAll(filepath.Join(logsDir, e.Name()))
		if rerr != nil {
			fmt.Fprintf(os.Stderr, "bench: WARNING reading %s: %v\n", e.Name(), rerr)
		}
		all = append(all, recs...)
	}

	// The checker's verdict covers acknowledged publishes only; the harness-health
	// guard fails the run when too little of the offered load was acknowledged for
	// that verdict to mean anything (report.HarnessSound).
	harnessOK, harnessFault := report.HarnessSound(res.Check.ConfirmedPublished, res.Check.UnconfirmedPublished)
	// Connection establishment is already outside the measured window: scenario.Run
	// starts EVERY subscriber before the publisher, at any connection count. The only
	// transient left is pipeline cold-start (first produce/consume, cold caches),
	// which measurement localises to the first second — hence a short declared warmup.
	var cutoff int64
	if cfg.Warmup > 0 {
		cutoff = t0.Add(cfg.Warmup).UnixNano()
	}
	latency, warmupExcluded := report.LatenciesWindowed(all, cutoff)
	result := report.Result{
		RunID:                id,
		Scenario:             filepath.Base(*scenarioPath),
		Pass:                 res.Check.Pass() && harnessOK,
		Latency:              latency,
		Holes:                len(res.Check.Holes),
		ConfirmedPublishes:   res.Check.ConfirmedPublished,
		UnconfirmedPublishes: res.Check.UnconfirmedPublished,
		HarnessFault:         harnessFault,
		WarmupExcluded:       warmupExcluded,
	}
	if cfg.Warmup > 0 {
		result.Warmup = cfg.Warmup.String()
	}
	for i, h := range res.Check.Holes {
		if i >= holesSampleMax {
			break
		}
		result.HolesSample = append(result.HolesSample,
			fmt.Sprintf("%s/%s seqs %d-%d", h.Subscriber, h.Channel, h.FromSeq, h.ToSeq))
	}

	resultPath := filepath.Join(*outDir, "result.json")
	out, err := os.Create(resultPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: create result: %v\n", err)
		return 2
	}
	werr := report.Write(out, result)
	cerr := out.Close()
	if werr != nil || cerr != nil {
		fmt.Fprintf(os.Stderr, "bench: write result: %v %v\n", werr, cerr)
		return 2
	}

	fmt.Printf("bench: run %s — pass=%v confirmed=%d unconfirmed=%d delivered=%d warmup=%v(-%d) p50=%v p99=%v holes=%d → %s\n",
		id, result.Pass, result.ConfirmedPublishes, result.UnconfirmedPublishes,
		result.Latency.Count, cfg.Warmup, warmupExcluded,
		result.Latency.P50, result.Latency.P99, result.Holes, resultPath)
	if harnessFault != "" {
		fmt.Fprintf(os.Stderr, "bench: HARNESS FAULT: %s\n", harnessFault)
	}
	if !result.Pass {
		return 1
	}
	return 0
}

// sleepUntil is the production SleepFunc: wait until an absolute time or ctx.
func sleepUntil(ctx context.Context, until time.Time) error {
	d := time.Until(until)
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
