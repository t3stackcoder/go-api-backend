//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepRelay (spec 11.4, G5): a relay batch with five rows across two keys,
// committed in one transaction before the relay wakes, then a janitor
// sweep. Expect: every row eventually published, in seq order per key
// (duplicates in Redis permitted), the cursor consistent with the outbox
// and the stream, and the janitor removing nothing within retention.

var relayBumps = append(bumpCommands("k1", "rb1", 3), bumpCommands("k2", "rb2", 2)...)

type relayState struct {
	janitor *pg.Janitor
}

// newRelay builds the relay of a node on its auxiliary pool.
func newRelay(c *cellRun, n *node, groups []string) *pg.Relay {
	return pg.NewRelay(n.aux, redisx.NewStreams(n.client, n.cfg), pg.RelayConfig{
		Topics: []string{workload.TopicBumped}, Partitions: partitions, BatchSize: 100, PollInterval: relayPoll,
		MinBackoff: 50 * time.Millisecond, MaxBackoff: 500 * time.Millisecond,
		KnownGroups: func() []string { return groups }, Logger: c.logger,
	})
}

var scenarioRelay = &scenario{
	name:     "SweepRelay",
	patterns: []string{"pg.relay.*", "redis.xadd", "redis.stream.*", "pg.janitor.delete"},
	groups:   []string{},
	variants: []string{"pool1", "stmt50", "oom"},
	newState: func() any { return &relayState{} },
	build: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*relayState)
		n, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), workload: true})
		if err != nil {
			return err
		}
		n.add("relay", newRelay(c, n, []string{workload.GroupReadModel}))
		st.janitor = pg.NewJanitor(n.aux, pg.JanitorConfig{
			Trimmer: redisx.NewStreams(n.client, n.cfg), Topics: []string{workload.TopicBumped}, Partitions: partitions, Logger: c.logger,
		})
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*relayState)
		n := c.nodes[0]
		if err := relayPublish(ctx, c, n, 3); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		werr := waitFor(ctx, runWait, drained(n.pool, n.client, n.cfg, nil))
		if _, err := janitorSweep(ctx, st.janitor); err != nil {
			c.note("janitor: %v", err)
		}
		// Let the poll run the data-loss check once with a cursor present.
		time.Sleep(2*relayPoll + 100*time.Millisecond)
		return werr
	},
	recover: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*relayState)
		n := c.nodes[0]
		// The client's publish is retried too: a crash before the relay's
		// first step may have preceded it. Keyed commands replay.
		if err := relayPublish(ctx, c, n, 8); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		if err := waitFor(ctx, recoverWait, drained(n.pool, n.client, n.cfg, nil)); err != nil {
			return err
		}
		if _, err := janitorSweep(ctx, st.janitor); err != nil {
			return fmt.Errorf("janitor: %w", err)
		}
		return nil
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		v.expectCount(ctx, int64(len(relayBumps)), "outbox rows", `SELECT count(*) FROM mediator_outbox`)
		v.expectCount(ctx, int64(len(relayBumps)), "published rows", `SELECT count(*) FROM mediator_outbox WHERE published_at IS NOT NULL`)
		checkStreamOrder(ctx, v, map[string]int64{"k1": 3, "k2": 2})
		checkCursor(ctx, v)
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		exp := map[string]invariants.Expect{}
		expectBumps(exp, relayBumps...)
		return exp
	},
}

// relayPublish commits the five keyed Bumps in one transaction so the relay
// sees one batch, retrying the whole transaction on error.
func relayPublish(ctx context.Context, c *cellRun, n *node, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = pg.WithTx(ctx, n.store, pg.TxOptions{}, func(tctx context.Context) error {
			for _, b := range relayBumps {
				if _, err := c.send(tctx, n.m, b); err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			return nil
		}
		c.note("publish attempt %d: %v", i+1, err)
		time.Sleep(150 * time.Millisecond)
	}
	return err
}

// janitorSweep runs one retention pass under a bounded context, so a
// timeout fault at pg.janitor.delete costs the deadline, not the cell.
func janitorSweep(ctx context.Context, j *pg.Janitor) (pg.SweepResult, error) {
	sctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	return j.Sweep(sctx)
}

// checkStreamOrder asserts that, per key, the first occurrence of every
// event in the partition stream is in seq order 1..n and that exactly n
// distinct events of the key were delivered.
func checkStreamOrder(ctx context.Context, v *verifier, want map[string]int64) {
	v.t.Helper()
	stream := v.cfg.Keys().Stream(workload.TopicBumped, 0)
	msgs, err := v.client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		v.t.Errorf("xrange %s: %v", stream, err)
		return
	}
	seen := map[string]map[string]bool{}
	next := map[string]int64{}
	for _, m := range msgs {
		env, _, _, derr := redisx.DecodeEntry(m.Values)
		if derr != nil {
			v.t.Errorf("undecodable stream entry %s: %v", m.ID, derr)
			continue
		}
		if seen[env.StreamKey] == nil {
			seen[env.StreamKey] = map[string]bool{}
		}
		if seen[env.StreamKey][env.ID.String()] {
			continue // a duplicate of an already delivered event
		}
		seen[env.StreamKey][env.ID.String()] = true
		next[env.StreamKey]++
		if env.Seq != next[env.StreamKey] {
			v.t.Errorf("key %s: first delivery of seq %d came at position %d", env.StreamKey, env.Seq, next[env.StreamKey])
		}
	}
	for key, n := range want {
		if int64(len(seen[key])) != n {
			v.t.Errorf("key %s: %d distinct events in the stream, want %d", key, len(seen[key]), n)
		}
	}
}

// checkCursor asserts the relay cursor names the highest published outbox
// row and a stream entry carrying that row.
func checkCursor(ctx context.Context, v *verifier) {
	v.t.Helper()
	var lastOutbox int64
	var lastStream string
	err := v.pool.QueryRow(ctx, `SELECT last_outbox_id, last_stream_id FROM mediator_relay_cursor WHERE topic = $1 AND partition = 0`, workload.TopicBumped).Scan(&lastOutbox, &lastStream)
	if err != nil {
		v.t.Errorf("relay cursor: %v", err)
		return
	}
	maxID := v.count(ctx, `SELECT coalesce(max(id), 0) FROM mediator_outbox WHERE published_at IS NOT NULL`)
	if lastOutbox != maxID {
		v.t.Errorf("cursor last_outbox_id=%d, highest published id=%d", lastOutbox, maxID)
	}
	msgs, err := v.client.XRange(ctx, v.cfg.Keys().Stream(workload.TopicBumped, 0), lastStream, lastStream).Result()
	if err != nil || len(msgs) != 1 {
		v.t.Errorf("cursor last_stream_id=%s not in the stream (%d entries, %v)", lastStream, len(msgs), err)
		return
	}
	if _, _, outboxID, derr := redisx.DecodeEntry(msgs[0].Values); derr != nil || outboxID != lastOutbox {
		v.t.Errorf("stream entry at the cursor carries outbox_id=%d (%v), cursor says %d", outboxID, derr, lastOutbox)
	}
}

func TestSweepRelay(t *testing.T) { runSweep(t, scenarioRelay) }
