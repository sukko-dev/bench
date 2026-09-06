// Package config parses a scenario TOML into a validated run configuration
// and generates the channel set. Every parameter a published run used is in
// this file, so the committed config is the run's disclosed provenance.
package config

import (
	"fmt"
	"io"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/sukko-dev/bench/internal/scenario"
)

// tomlConfig is the on-disk shape.
type tomlConfig struct {
	Tenant         string      `toml:"tenant"`
	ChannelPrefix  string      `toml:"channel_prefix"`
	Channels       int         `toml:"channels"`
	SubsPerChannel int         `toml:"subs_per_channel"`
	BaselineRate   float64     `toml:"baseline_rate"`
	Duration       string      `toml:"duration"`
	PayloadSize    int         `toml:"payload_size"`
	Bursts         []tomlBurst `toml:"bursts"`
}

type tomlBurst struct {
	StartFraction float64 `toml:"start_fraction"`
	Duration      string  `toml:"duration"`
	Multiplier    float64 `toml:"multiplier"`
}

// Config is the validated scenario plus the generated channel set. It carries
// everything scenario.Config needs except the runtime URL/Token/RunID, which
// come from flags at launch.
type Config struct {
	Tenant         string
	Channels       []string
	SubsPerChannel int
	BaselineRate   float64
	Duration       time.Duration
	PayloadSize    int
	Bursts         []scenario.Burst
}

// Parse reads and validates a scenario TOML.
func Parse(r io.Reader) (Config, error) {
	var tc tomlConfig
	if _, err := toml.NewDecoder(r).Decode(&tc); err != nil {
		return Config{}, fmt.Errorf("parse scenario toml: %w", err)
	}
	if tc.Tenant == "" {
		return Config{}, fmt.Errorf("tenant must be set")
	}
	if tc.Channels <= 0 {
		return Config{}, fmt.Errorf("channels must be positive, got %d", tc.Channels)
	}
	if tc.SubsPerChannel <= 0 {
		return Config{}, fmt.Errorf("subs_per_channel must be positive, got %d", tc.SubsPerChannel)
	}
	if tc.BaselineRate <= 0 {
		return Config{}, fmt.Errorf("baseline_rate must be positive, got %v", tc.BaselineRate)
	}
	if tc.PayloadSize <= 0 {
		return Config{}, fmt.Errorf("payload_size must be positive, got %d", tc.PayloadSize)
	}
	dur, err := time.ParseDuration(tc.Duration)
	if err != nil {
		return Config{}, fmt.Errorf("duration: %w", err)
	}
	if dur <= 0 {
		return Config{}, fmt.Errorf("duration must be positive, got %v", dur)
	}
	prefix := tc.ChannelPrefix
	if prefix == "" {
		prefix = "ch"
	}

	c := Config{
		Tenant:         tc.Tenant,
		SubsPerChannel: tc.SubsPerChannel,
		BaselineRate:   tc.BaselineRate,
		Duration:       dur,
		PayloadSize:    tc.PayloadSize,
	}
	for i := range tc.Channels {
		c.Channels = append(c.Channels, fmt.Sprintf("%s.%s-%d", tc.Tenant, prefix, i))
	}
	for _, b := range tc.Bursts {
		bd, err := time.ParseDuration(b.Duration)
		if err != nil {
			return Config{}, fmt.Errorf("burst duration: %w", err)
		}
		if b.Multiplier <= 0 || bd <= 0 || b.StartFraction < 0 || b.StartFraction >= 1 {
			return Config{}, fmt.Errorf("invalid burst %+v", b)
		}
		c.Bursts = append(c.Bursts, scenario.Burst{StartFraction: b.StartFraction, Duration: bd, Multiplier: b.Multiplier})
	}
	return c, nil
}
