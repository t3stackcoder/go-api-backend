//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// controller is the harness side of spec 11.6: it speaks HTTP to the nodes,
// the Toxiproxy API, and the Docker CLI, and reads Postgres and Redis
// directly (never through the nodes) for the invariants and the reset.
type controller struct {
	cfg    config
	log    *slog.Logger
	nodes  []*node
	docker dockerCLI
	toxi   *toxiproxy
	pool   *pgxpool.Pool
	rdb    *redis.Client
	rcfg   redisx.Config
	runDir string

	mu         sync.Mutex
	remoteMode bool   // remote-send: routes dispatch remotely on every node
	serving    int    // index of the node serving remote commands
	snapshot   string // host path of the Redis appendonlydir snapshot
}

func newController(ctx context.Context, cfg config, log *slog.Logger, runDir string) (*controller, error) {
	c := &controller{cfg: cfg, log: log, docker: dockerCLI{log: log}, toxi: newToxiproxy(cfg.ToxiproxyURL), runDir: runDir}
	for i, addr := range cfg.NodeAddrs {
		c.nodes = append(c.nodes, newNode(i, addr, cfg.ContainerPrefix, cfg.RequestTimeout))
	}
	pool, err := pgxpool.New(ctx, cfg.PGURL)
	if err != nil {
		return nil, fmt.Errorf("chaos: postgres: %w", err)
	}
	c.pool = pool
	if err := c.waitPG(ctx, 30*time.Second); err != nil {
		pool.Close()
		return nil, err
	}
	c.rcfg = redisx.Config{Addr: cfg.RedisAddr, Prefix: "mediator", PartitionsPerTopic: cfg.Partitions}.WithDefaults()
	c.rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DialTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second})
	if err := c.waitRedis(ctx, 30*time.Second); err != nil {
		c.close()
		return nil, err
	}
	proxies, err := c.toxi.proxies(ctx)
	if err != nil {
		c.close()
		return nil, err
	}
	have := map[string]bool{}
	for _, p := range proxies {
		have[p] = true
	}
	for _, n := range c.nodes {
		if !have[n.PGProxy] || !have[n.RedisProxy] {
			c.close()
			return nil, fmt.Errorf("chaos: toxiproxy lacks %s or %s (have %s)", n.PGProxy, n.RedisProxy, strings.Join(proxies, ","))
		}
	}
	return c, nil
}

func (c *controller) close() {
	if c.pool != nil {
		c.pool.Close()
	}
	if c.rdb != nil {
		_ = c.rdb.Close()
	}
}

// container is the compose container name of a service.
func (c *controller) container(service string) string {
	return c.cfg.ContainerPrefix + "-" + service + "-1"
}

func (c *controller) nodeContainers() []string {
	out := make([]string, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, n.Container)
	}
	return out
}

// waitPG polls Postgres until it answers.
func (c *controller) waitPG(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := c.pool.Ping(pctx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("chaos: postgres not ready within %s: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// waitRedis polls Redis until it answers.
func (c *controller) waitRedis(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := c.rdb.Ping(pctx).Err()
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("chaos: redis not ready within %s: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// reset gives the run a clean baseline: no toxics, every node stopped
// (gracefully), the framework and workload tables truncated, the bank
// seeded, Redis flushed, then every node started and ready.
func (c *controller) reset(ctx context.Context) error {
	c.log.Info("reset: removing toxics, stopping nodes, truncating stores")
	if err := c.toxi.reset(ctx); err != nil {
		return err
	}
	for _, n := range c.nodes {
		if st, err := c.docker.state(ctx, n.Container); err == nil && st.Paused {
			_ = c.docker.unpause(ctx, n.Container)
		}
	}
	if err := c.docker.stop(ctx, 25*time.Second, c.nodeContainers()...); err != nil {
		return err
	}
	for _, svc := range []string{"postgres", "redis"} {
		if err := c.docker.ensureRunning(ctx, c.container(svc)); err != nil {
			return err
		}
	}
	if err := c.waitPG(ctx, 60*time.Second); err != nil {
		return err
	}
	if err := c.waitRedis(ctx, 60*time.Second); err != nil {
		return err
	}
	if _, err := c.pool.Exec(ctx, `TRUNCATE mediator_outbox, mediator_stream_seq, mediator_inbox, mediator_idempotency, mediator_relay_cursor RESTART IDENTITY`); err != nil {
		return fmt.Errorf("chaos: truncate framework tables: %w", err)
	}
	if err := workload.Truncate(ctx, c.pool); err != nil {
		return err
	}
	if err := workload.SeedBank(ctx, c.pool); err != nil {
		return err
	}
	if err := c.rdb.FlushAll(ctx).Err(); err != nil {
		return fmt.Errorf("chaos: flushall: %w", err)
	}
	for _, n := range c.nodes {
		if err := c.docker.start(ctx, n.Container); err != nil {
			return err
		}
	}
	for _, n := range c.nodes {
		if err := n.waitReady(ctx, 120*time.Second); err != nil {
			return err
		}
	}
	c.log.Info("reset: nodes ready")
	return nil
}

// setRemoteMode records the dispatch topology: remote-send puts every node
// in remote dispatch with exactly one node serving.
func (c *controller) setRemoteMode(remote bool, serving int) {
	c.mu.Lock()
	c.remoteMode, c.serving = remote, serving
	c.mu.Unlock()
}

// servingNode is the index of the node that serves remote commands.
func (c *controller) servingNode() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serving
}

// rotateServing moves the serving flag to another node (handler-rotate)
// and returns the old and new indexes.
func (c *controller) rotateServing(next int) (old, cur int) {
	c.mu.Lock()
	old = c.serving
	c.serving = next
	c.mu.Unlock()
	return old, next
}

// applyMode configures one node's dispatch mode, remote serving flag, relay
// switch, and clock for the workload. It is applied at the start and every
// time a node comes back from a restart.
func (c *controller) applyMode(ctx context.Context, n *node) error {
	c.mu.Lock()
	remote, serving := c.remoteMode, c.serving
	c.mu.Unlock()
	var errs []error
	if remote {
		errs = append(errs, n.setHandlers(ctx, "remote", n.Index == serving))
	} else {
		errs = append(errs, n.setHandlers(ctx, "local", true))
	}
	errs = append(errs, n.setRelay(ctx, true), n.setClock(ctx, 0))
	return errors.Join(errs...)
}

func (c *controller) applyModes(ctx context.Context) error {
	var errs []error
	for _, n := range c.nodes {
		errs = append(errs, c.applyMode(ctx, n))
	}
	return errors.Join(errs...)
}

// restoreNode brings a killed or stopped node back: start, wait for
// readiness, reapply its mode.
func (c *controller) restoreNode(ctx context.Context, n *node) error {
	if err := c.docker.ensureRunning(ctx, n.Container); err != nil {
		return err
	}
	if err := n.waitReady(ctx, 120*time.Second); err != nil {
		return err
	}
	return c.applyMode(ctx, n)
}

// heal removes every toxic, unpauses and starts everything, waits for the
// stores and nodes, and resets clocks, relays, and handler placement.
func (c *controller) heal(ctx context.Context) error {
	c.log.Info("heal: removing toxics, restoring containers")
	var errs []error
	if err := c.toxi.reset(ctx); err != nil {
		errs = append(errs, err)
	}
	for _, svc := range []string{"postgres", "redis"} {
		if err := c.docker.ensureRunning(ctx, c.container(svc)); err != nil {
			errs = append(errs, err)
		}
	}
	if err := c.waitPG(ctx, 60*time.Second); err != nil {
		errs = append(errs, err)
	}
	if err := c.waitRedis(ctx, 60*time.Second); err != nil {
		errs = append(errs, err)
	}
	for _, n := range c.nodes {
		if err := c.docker.ensureRunning(ctx, n.Container); err != nil {
			errs = append(errs, err)
		}
	}
	for _, n := range c.nodes {
		if err := n.waitReady(ctx, 120*time.Second); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := c.applyMode(ctx, n); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// snapshotRedis rewrites the AOF and copies Redis's data directory to the
// run directory for redis-restore-old.
func (c *controller) snapshotRedis(ctx context.Context) error {
	redisC := c.container("redis")
	if _, err := c.docker.exec(ctx, redisC, "redis-cli", "BGREWRITEAOF"); err != nil && !strings.Contains(err.Error(), "already in progress") {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		info, err := c.rdb.Info(ctx, "persistence").Result()
		if err == nil && strings.Contains(info, "aof_rewrite_in_progress:0") && !strings.Contains(info, "aof_rewrite_scheduled:1") {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("chaos: aof rewrite did not finish: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	dir := filepath.Join(c.runDir, "redis-snapshot")
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	if err := c.docker.cp(ctx, redisC+":/data/appendonlydir", dir); err != nil {
		return err
	}
	c.mu.Lock()
	c.snapshot = filepath.Join(dir, "appendonlydir")
	c.mu.Unlock()
	c.log.Info("redis snapshot taken", "dir", dir)
	return nil
}

func (c *controller) hasSnapshot() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot != ""
}

// restoreRedis stops Redis, replaces its append-only directory with the
// snapshot, and starts it again (redis-restore-old).
func (c *controller) restoreRedis(ctx context.Context) error {
	c.mu.Lock()
	snap := c.snapshot
	c.mu.Unlock()
	if snap == "" {
		return errors.New("chaos: no redis snapshot")
	}
	redisC := c.container("redis")
	if err := c.docker.stop(ctx, 5*time.Second, redisC); err != nil {
		return err
	}
	if err := c.docker.cp(ctx, snap, redisC+":/data/"); err != nil {
		_ = c.docker.start(ctx, redisC)
		return err
	}
	if err := c.docker.start(ctx, redisC); err != nil {
		return err
	}
	return c.waitRedis(ctx, 60*time.Second)
}

// env is the invariants environment of the run.
func (c *controller) env(bound time.Duration) invariants.Env {
	return invariants.Env{
		Pool: c.pool, Redis: c.rdb, Cfg: c.rcfg,
		Groups: workload.DefaultGroups, Topics: []string{workload.TopicBumped},
		Partitions: c.cfg.Partitions, Bound: bound,
	}
}

// relayReplayed sums the relay replay counters of the live nodes (G12).
func (c *controller) relayReplayed(ctx context.Context) int64 {
	var total int64
	for _, n := range c.nodes {
		st, err := n.stats(ctx)
		if err != nil {
			continue
		}
		if relay, ok := st["relay"].(map[string]any); ok {
			if stats, ok := relay["stats"].(map[string]any); ok {
				if f, ok := stats["replayed"].(float64); ok {
					total += int64(f)
				}
			}
		}
	}
	return total
}

// goroutineCounts reads /chaos/goroutines of every live node.
func (c *controller) goroutineCounts(ctx context.Context) map[string]int {
	out := map[string]int{}
	for _, n := range c.nodes {
		if g, err := n.goroutines(ctx); err == nil {
			out[n.ID] = g
		}
	}
	return out
}
