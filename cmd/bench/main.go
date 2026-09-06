// Command bench runs one benchmark scenario against a live Sukko gateway and
// writes the result artifact. Provisioning and token minting are the harness's
// job (see bootstrap.sh / the Makefile) — this binary takes a gateway URL and
// a ready token as inputs.
//
// Usage:
//
//	bench --ws ws://host:3000 --http http://host:3000 --token <jwt> \
//	      --scenario scenarios/odds-burst.toml --out results/<dir> [--run-id ID]
//
// Exit codes: 0 the run completed and the zero-loss checker PASSED; 1 the run
// completed but the checker FAILED (loss/phantom/misrouting — a published
// result); 2 the run could not execute.
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
	warmup := flag.Duration("warmup", 60*time.Second, "warmup excluded from the reported window (informational; the driver runs the full duration)")
	flag.Parse()

	if *wsURL == "" || *httpURL == "" || *token == "" || *scenarioPath == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "bench: --ws, --http, --token, --scenario, and --out are all required")
		return 2
	}
	_ = *warmup // recorded in the result metadata; windowing is applied by the analysis step

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
				T0:          time.Now(),
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
		T0:             time.Now(),
	}, deps)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: run failed: %v\n", err)
		return 2
	}

	// Latency over every subscriber's received records.
	var all []rlog.Record
	entries, _ := os.ReadDir(logsDir)
	for _, e := range entries {
		recs, rerr := rlog.ReadAll(filepath.Join(logsDir, e.Name()))
		if rerr != nil {
			fmt.Fprintf(os.Stderr, "bench: WARNING reading %s: %v\n", e.Name(), rerr)
		}
		all = append(all, recs...)
	}

	result := report.Result{
		RunID:    id,
		Scenario: filepath.Base(*scenarioPath),
		Pass:     res.Check.Pass(),
		Latency:  report.Latencies(all),
		Holes:    len(res.Check.Holes),
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

	fmt.Printf("bench: run %s — pass=%v delivered=%d p50=%v p99=%v holes=%d → %s\n",
		id, result.Pass, result.Latency.Count, result.Latency.P50, result.Latency.P99, result.Holes, resultPath)
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
