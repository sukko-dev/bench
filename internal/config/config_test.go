package config

import (
	"strings"
	"testing"
	"time"
)

const sample = `
tenant = "bench"
channel_prefix = "md"
channels = 3
subs_per_channel = 2
baseline_rate = 2.0
duration = "10s"
payload_size = 300

[[bursts]]
start_fraction = 0.5
duration = "3s"
multiplier = 8
`

func TestParseFillsScenarioAndGeneratesChannels(t *testing.T) {
	c, err := Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// Channels are {tenant}.{prefix}-{n}, zero-padded is not required but the
	// count and tenant-prefixing are.
	if len(c.Channels) != 3 {
		t.Fatalf("channels = %v, want 3", c.Channels)
	}
	for _, ch := range c.Channels {
		if !strings.HasPrefix(ch, "bench.") {
			t.Errorf("channel %q not tenant-prefixed", ch)
		}
	}
	if c.SubsPerChannel != 2 || c.BaselineRate != 2.0 || c.PayloadSize != 300 {
		t.Errorf("scalars wrong: %+v", c)
	}
	if c.Duration != 10*time.Second {
		t.Errorf("duration = %v, want 10s", c.Duration)
	}
	if len(c.Bursts) != 1 || c.Bursts[0].Multiplier != 8 || c.Bursts[0].Duration != 3*time.Second || c.Bursts[0].StartFraction != 0.5 {
		t.Errorf("burst wrong: %+v", c.Bursts)
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	tests := []struct{ name, toml string }{
		{"zero channels", `tenant="b"` + "\n" + `channels=0` + "\n" + `subs_per_channel=1` + "\n" + `baseline_rate=1` + "\n" + `duration="1s"` + "\n" + `payload_size=100`},
		{"zero subs", `tenant="b"` + "\n" + `channels=1` + "\n" + `subs_per_channel=0` + "\n" + `baseline_rate=1` + "\n" + `duration="1s"` + "\n" + `payload_size=100`},
		{"empty tenant", `tenant=""` + "\n" + `channels=1` + "\n" + `subs_per_channel=1` + "\n" + `baseline_rate=1` + "\n" + `duration="1s"` + "\n" + `payload_size=100`},
		{"bad duration", `tenant="b"` + "\n" + `channels=1` + "\n" + `subs_per_channel=1` + "\n" + `baseline_rate=1` + "\n" + `duration="soon"` + "\n" + `payload_size=100`},
		{"nonpositive rate", `tenant="b"` + "\n" + `channels=1` + "\n" + `subs_per_channel=1` + "\n" + `baseline_rate=0` + "\n" + `duration="1s"` + "\n" + `payload_size=100`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse(strings.NewReader(tt.toml)); err == nil {
				t.Fatalf("Parse accepted invalid config: %s", tt.name)
			}
		})
	}
}

func TestChannelNamesAreUnique(t *testing.T) {
	c, err := Parse(strings.NewReader(`
tenant = "bench"
channel_prefix = "md"
channels = 500
subs_per_channel = 1
baseline_rate = 1
duration = "1s"
payload_size = 100
`))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, ch := range c.Channels {
		if seen[ch] {
			t.Fatalf("duplicate channel %q", ch)
		}
		seen[ch] = true
	}
	if len(seen) != 500 {
		t.Fatalf("got %d unique channels, want 500", len(seen))
	}
}
