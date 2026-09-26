//go:build chaos

package chaos

import (
	"encoding/json/v2"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Flags of TestChaos and TestReplay. Every default comes from the
// environment first (CHAOS_*), so tools/task can pass either.
var (
	flagWorkload  = flag.String("workload", envOr("CHAOS_WORKLOAD", "register"), "workload of spec 11.6")
	flagSeed      = flag.String("seed", envOr("CHAOS_SEED", "1"), "seed of the nemesis generator")
	flagDuration  = flag.Duration("duration", envDur("CHAOS_DURATION", 5*time.Minute), "mayhem duration")
	flagNodes     = flag.Int("nodes", envInt("CHAOS_NODES", 3), "number of chaos nodes")
	flagRunDir    = flag.String("run-dir", envOr("CHAOS_RUN_DIR", "runs"), "parent directory of run directories")
	flagBound     = flag.Duration("recovery-bound", envDur("CHAOS_RECOVERY_BOUND", 60*time.Second), "RecoveryBound of G15 (L1, L2)")
	flagNemRate   = flag.Duration("nemesis-rate", envDur("CHAOS_NEMESIS_RATE", 0), "mean interval between nemeses (default 10s, soak 60s)")
	flagSoak      = flag.Bool("soak", envBool("CHAOS_SOAK", false), "soak mode: low nemesis rate, invariant checks every 5 minutes")
	flagClients   = flag.Int("clients", envInt("CHAOS_CLIENTS", 2), "logical clients per node")
	flagRate      = flag.Float64("rate", envFloat("CHAOS_RATE", 5), "operations per second per client")
	flagWarmup    = flag.Duration("warmup", envDur("CHAOS_WARMUP", 10*time.Second), "warm-up before the first nemesis")
	flagCheckTO   = flag.Duration("check-timeout", envDur("CHAOS_CHECK_TIMEOUT", 10*time.Minute), "Porcupine time budget")
	flagReqTO     = flag.Duration("request-timeout", envDur("CHAOS_REQUEST_TIMEOUT", 10*time.Second), "client timeout of one workload request; beyond it the op is info")
	flagReplayDir = flag.String("replay-dir", envOr("CHAOS_REPLAY_DIR", ""), "run directory for TestReplay")
)

// config is the effective configuration of one run; it is written to
// config.json in the run directory. Durations are mirrored as strings
// because time.Duration has no JSON encoding.
type config struct {
	Workload        string        `json:"workload"`
	Seed            int64         `json:"seed"`
	Duration        time.Duration `json:"-"`
	DurationText    string        `json:"duration"`
	Nodes           int           `json:"nodes"`
	RunParent       string        `json:"run_parent"`
	RecoveryBound   time.Duration `json:"-"`
	BoundText       string        `json:"recovery_bound"`
	NemesisInterval time.Duration `json:"-"`
	NemesisText     string        `json:"nemesis_interval"`
	Soak            bool          `json:"soak"`
	Clients         int           `json:"clients_per_node"`
	Rate            float64       `json:"rate_per_client"`
	Warmup          time.Duration `json:"-"`
	WarmupText      string        `json:"warmup"`
	CheckTimeout    time.Duration `json:"-"`
	RequestTimeout  time.Duration `json:"-"`
	RequestText     string        `json:"request_timeout"`
	NodeAddrs       []string      `json:"node_addrs"`
	PGURL           string        `json:"pg_url"`
	RedisAddr       string        `json:"redis_addr"`
	ToxiproxyURL    string        `json:"toxiproxy_url"`
	ContainerPrefix string        `json:"container_prefix"`
	Partitions      int           `json:"partitions"`
	// DrainBound is how long a graceful restart may take to exit 0 (G16).
	DrainBound time.Duration `json:"-"`
	DrainText  string        `json:"drain_bound"`
	// SoakCheckEvery is the interval of the soak invariant checks (11.7).
	SoakCheckEvery time.Duration `json:"-"`
}

// loadConfig reads the flags (already parsed by the test binary).
func loadConfig() (config, error) {
	seed, err := strconv.ParseInt(*flagSeed, 10, 64)
	if err != nil {
		return config{}, fmt.Errorf("chaos: -seed %q: %w", *flagSeed, err)
	}
	c := config{
		Workload:        *flagWorkload,
		Seed:            seed,
		Duration:        *flagDuration,
		Nodes:           *flagNodes,
		RunParent:       *flagRunDir,
		RecoveryBound:   *flagBound,
		NemesisInterval: *flagNemRate,
		Soak:            *flagSoak,
		Clients:         *flagClients,
		Rate:            *flagRate,
		Warmup:          *flagWarmup,
		CheckTimeout:    *flagCheckTO,
		RequestTimeout:  *flagReqTO,
		PGURL:           envOr("CHAOS_PG_URL", "postgres://app:app@localhost:5432/app"),
		RedisAddr:       envOr("CHAOS_REDIS_ADDR", "localhost:6379"),
		ToxiproxyURL:    envOr("CHAOS_TOXIPROXY_URL", "http://localhost:8474"),
		ContainerPrefix: envOr("CHAOS_CONTAINER_PREFIX", "go-api-backend"),
		Partitions:      envInt("CHAOS_PARTITIONS", 16),
		DrainBound:      envDur("CHAOS_DRAIN_BOUND", 30*time.Second),
		SoakCheckEvery:  envDur("CHAOS_SOAK_CHECK_EVERY", 5*time.Minute),
	}
	if c.Nodes <= 0 {
		return c, fmt.Errorf("chaos: -nodes must be positive")
	}
	if c.Clients <= 0 || c.Rate <= 0 {
		return c, fmt.Errorf("chaos: -clients and -rate must be positive")
	}
	if c.NemesisInterval <= 0 {
		c.NemesisInterval = 10 * time.Second
		if c.Soak {
			c.NemesisInterval = 60 * time.Second
		}
	}
	if raw := os.Getenv("CHAOS_NODE_ADDRS"); raw != "" {
		for _, a := range strings.Split(raw, ",") {
			if a = strings.TrimSpace(a); a != "" {
				c.NodeAddrs = append(c.NodeAddrs, a)
			}
		}
		if len(c.NodeAddrs) != c.Nodes {
			return c, fmt.Errorf("chaos: CHAOS_NODE_ADDRS lists %d addresses for %d nodes", len(c.NodeAddrs), c.Nodes)
		}
	} else {
		for i := 0; i < c.Nodes; i++ {
			c.NodeAddrs = append(c.NodeAddrs, fmt.Sprintf("http://localhost:%d", 18081+i))
		}
	}
	c.DurationText = c.Duration.String()
	c.BoundText = c.RecoveryBound.String()
	c.NemesisText = c.NemesisInterval.String()
	c.WarmupText = c.Warmup.String()
	c.RequestText = c.RequestTimeout.String()
	c.DrainText = c.DrainBound.String()
	return c, nil
}

func (c config) marshal() ([]byte, error) {
	return json.Marshal(c, json.Deterministic(true))
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envDur(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(name string, def float64) float64 {
	if v := os.Getenv(name); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(name string, def bool) bool {
	if v := os.Getenv(name); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
