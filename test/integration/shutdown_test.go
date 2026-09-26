//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/examples/orders/orders"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// orderTopics are the topics the service's consumers subscribe to.
var orderTopics = []string{"OrderSubmitted"}

// TestRuntime_Shutdown_DrainsInFlight is G16 for the HTTP stage: a node
// asked to stop finishes the request it is serving, refuses or completes
// (never hangs) requests that arrive afterwards, releases its leases, and
// exits with code 0 within the drain timeout.
func TestRuntime_Shutdown_DrainsInFlight(t *testing.T) {
	db := newDatabase(t)
	n := startNode(t, nodeConfig{db: db, env: map[string]string{"ORDERS_DEBUG": "1"}})
	n.waitReady(t, 60*time.Second)
	rc := n.redisClient(t)
	eventually(t, 30*time.Second, "the node to own a lease", func() bool {
		return len(n.leasesHeldBy(t, rc, orders.Groups)) > 0
	})

	c := n.client(token(t))
	const sleepMs = 3000
	type outcome struct {
		resp response
		err  error
	}
	inflight := make(chan outcome, 1)
	go func() {
		resp, err := c.try(http.MethodGet, "/debug/sleep?ms=3000", nil, nil)
		inflight <- outcome{resp, err}
	}()
	// Let the request reach the handler before asking for shutdown.
	time.Sleep(500 * time.Millisecond)
	select {
	case o := <-inflight:
		t.Fatalf("debug.Sleep answered before the shutdown request: %v %v", o.resp, o.err)
	default:
	}

	requested := time.Now()
	n.requestShutdown(t)

	// A request after the shutdown request is refused or completes; it
	// must not hang for the whole drain.
	late := make(chan outcome, 1)
	go func() {
		lc := n.client("")
		lc.http.Timeout = nodeDrainTimeout
		resp, err := lc.try(http.MethodGet, "/healthz", nil, nil)
		late <- outcome{resp, err}
	}()

	select {
	case o := <-inflight:
		if o.err != nil {
			t.Fatalf("in-flight request failed during drain: %v", o.err)
		}
		o.resp.expect(t, http.StatusOK, "in-flight debug.Sleep")
		var res struct {
			SleptMillis int    `json:"sleptMs"`
			Node        string `json:"node"`
		}
		o.resp.decode(t, &res)
		if res.SleptMillis != sleepMs || res.Node != n.id {
			t.Errorf("in-flight response %+v, want sleptMs=%d node=%s", res, sleepMs, n.id)
		}
	case <-time.After(nodeDrainTimeout + 5*time.Second):
		t.Fatal("in-flight request did not complete within the drain timeout")
	}

	select {
	case o := <-late:
		if o.err != nil {
			t.Logf("late request refused: %v", o.err)
		} else {
			t.Logf("late request completed: %d", o.resp.Status)
		}
	case <-time.After(nodeDrainTimeout + 5*time.Second):
		t.Error("a request sent after the shutdown request hung past the drain timeout")
	}

	code := n.waitExit(t, nodeDrainTimeout+10*time.Second)
	elapsed := time.Since(requested)
	if code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
	if elapsed > nodeDrainTimeout {
		t.Errorf("exit took %s, longer than the drain timeout %s", elapsed.Round(time.Millisecond), nodeDrainTimeout)
	}
	t.Logf("node exited with code %d after %s", code, elapsed.Round(time.Millisecond))

	if held := n.leasesHeldBy(t, rc, orders.Groups); len(held) != 0 {
		t.Errorf("leases still held by %s after exit: %+v", n.id, held)
	}
	pattern := n.rcfg.Keys().Prefix + ":lease:*"
	keys, err := rc.Keys(context.Background(), pattern).Result()
	if err != nil {
		t.Fatalf("KEYS %s: %v", pattern, err)
	}
	for _, k := range keys {
		v, _ := rc.Get(context.Background(), k).Result()
		if node, _, ok := redisx.ParseLeaseValue(v); ok && node == n.id {
			t.Errorf("lease key %s still names node %s", k, n.id)
		}
	}
}

// TestRuntime_Shutdown_AckAfterCommit is G7 and G16 for the consumer
// stage: a node stopped while the read-model projector is mid-apply
// finishes that entry, acknowledges it only after its transaction
// committed, and exits 0; a second node then finds nothing to redo, so the
// projection is applied exactly once with its inbox row present.
func TestRuntime_Shutdown_AckAfterCommit(t *testing.T) {
	db := newDatabase(t)
	prefix := uniquePrefix()
	const delay = 3 * time.Second
	a := startNode(t, nodeConfig{db: db, prefix: prefix, nodeID: "node-a-" + randomHex(2),
		env: map[string]string{"ORDERS_DEBUG_PROJECTOR_DELAY": delay.String()}})
	a.waitReady(t, 60*time.Second)
	rc := a.redisClient(t)
	eventually(t, 30*time.Second, "node a to own the read_model leases", func() bool {
		return len(a.leasesHeldBy(t, rc, []string{orders.GroupReadModel})) == nodePartitions
	})

	c := a.client(token(t, orders.PermissionOrdersWrite))
	var created struct {
		OrderID string `json:"orderId"`
	}
	c.do(t, http.MethodPost, "/orders", map[string]any{
		"customerId": "018f1c2e-4d3a-7b5c-9e8f-0a1b2c3d4e5f",
		"lines":      []map[string]any{{"sku": "SKU-A", "qty": 2, "unitPrice": 150}},
	}, nil).expect(t, http.StatusCreated, "create order").decode(t, &created)
	c.do(t, http.MethodPost, "/orders/"+created.OrderID+"/submit", nil, nil).expect(t, http.StatusNoContent, "submit order")

	var eventID string
	if err := db.pool.QueryRow(context.Background(),
		`SELECT event_id::text FROM mediator_outbox WHERE topic = 'OrderSubmitted' AND stream_key = $1`, created.OrderID).Scan(&eventID); err != nil {
		t.Fatalf("outbox row: %v", err)
	}

	// The projector has read the entry (it is pending for the group) and is
	// now inside its delay: stop the node in the middle of the apply.
	eventually(t, 30*time.Second, "the read_model group to have the entry pending", func() bool {
		return a.pendingFor(t, rc, orders.GroupReadModel, orderTopics) > 0
	})
	requested := time.Now()
	a.requestShutdown(t)
	code := a.waitExit(t, nodeDrainTimeout*3)
	elapsed := time.Since(requested)
	if code != 0 {
		t.Errorf("node a exit code %d, want 0", code)
	}
	if elapsed < delay {
		t.Errorf("node a exited after %s, before the in-flight projector delay of %s", elapsed.Round(time.Millisecond), delay)
	}
	t.Logf("node a exited with code %d after %s", code, elapsed.Round(time.Millisecond))

	pending := a.pendingFor(t, rc, orders.GroupReadModel, orderTopics)
	applied := db.count(t, `SELECT count(*) FROM order_summaries WHERE order_id = $1`, created.OrderID)
	inbox := db.count(t, `SELECT count(*) FROM mediator_inbox WHERE consumer_group = $1 AND event_id = $2`, orders.GroupReadModel, eventID)
	if (applied == 1) != (pending == 0) {
		t.Errorf("projection applied=%d but pending=%d: the row must exist exactly when the entry was acknowledged", applied, pending)
	}
	if applied != inbox {
		t.Errorf("projection rows %d but inbox rows %d: they commit together", applied, inbox)
	}
	if pending != 0 || applied != 1 {
		t.Errorf("graceful shutdown must finish the in-flight entry: pending=%d applied=%d", pending, applied)
	}
	if held := a.leasesHeldBy(t, rc, orders.Groups); len(held) != 0 {
		t.Errorf("leases still held by node a after exit: %+v", held)
	}

	b := startNode(t, nodeConfig{db: db, prefix: prefix, nodeID: "node-b-" + randomHex(2)})
	b.waitReady(t, 60*time.Second)
	eventually(t, 30*time.Second, "node b to own every lease", func() bool {
		return len(b.leasesHeldBy(t, rc, orders.Groups)) == nodePartitions*len(orders.Groups)
	})
	eventually(t, 30*time.Second, "the inventory consumer to apply the event", func() bool {
		return db.count(t, `SELECT count(*) FROM reservations WHERE order_id = $1`, created.OrderID) == 1
	})
	// Give node b time to redeliver anything left pending before checking.
	time.Sleep(2 * time.Second)
	if n := b.pendingFor(t, rc, orders.GroupReadModel, orderTopics) + b.pendingFor(t, rc, orders.GroupInventory, orderTopics); n != 0 {
		t.Errorf("%d entries still pending after node b caught up", n)
	}
	if n := db.count(t, `SELECT count(*) FROM order_summaries WHERE order_id = $1 AND status = 'submitted' AND total = 300 AND last_seq = 1`, created.OrderID); n != 1 {
		t.Errorf("order_summaries rows for the order: %d, want exactly one submitted row with total 300 and last_seq 1", n)
	}
	if n := db.count(t, `SELECT count(*) FROM mediator_inbox WHERE event_id = $1`, eventID); n != int64(len(orders.Groups)) {
		t.Errorf("inbox rows for the event: %d, want one per group (%d)", n, len(orders.Groups))
	}
	if n := db.count(t, `SELECT count(*) FROM reservations WHERE order_id = $1`, created.OrderID); n != 1 {
		t.Errorf("reservations for the order: %d, want 1", n)
	}
	if n := db.count(t, `SELECT count(*) FROM audit_log WHERE order_id = $1`, created.OrderID); n != 1 {
		t.Errorf("audit rows for the order: %d, want 1", n)
	}
	b.requestShutdown(t)
	if code := b.waitExit(t, nodeDrainTimeout*3); code != 0 {
		t.Errorf("node b exit code %d, want 0", code)
	}
}
