//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/examples/orders/orders"
	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// customerID is the customer of every order the end-to-end test creates.
const customerID = "018f1c2e-4d3a-7b5c-9e8f-0a1b2c3d4e5f"

// TestOrders_EndToEnd drives the example service over HTTP as a client
// would: authentication and authorization as problem+json, an idempotent
// create, cache hits that survive invalidation (G9), a list that is never
// stale after a create, the submit that fans out to both durable consumers
// exactly once (G7), and the SSE stream of the order's events.
func TestOrders_EndToEnd(t *testing.T) {
	ctx := context.Background()
	db := newDatabase(t)
	n := startNode(t, nodeConfig{db: db})
	n.waitReady(t, 60*time.Second)
	rc := n.redisClient(t)
	if err := orders.SeedInventory(ctx, db.pool, map[string]int64{"SKU-A": 100, "SKU-B": 50}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	eventually(t, 30*time.Second, "the node to own every lease", func() bool {
		return len(n.leasesHeldBy(t, rc, orders.Groups)) == nodePartitions*len(orders.Groups)
	})

	writer := n.client(token(t, orders.PermissionOrdersWrite))
	reader := n.client(token(t))
	anon := n.client("")
	body := map[string]any{
		"customerId": customerID,
		"lines":      []map[string]any{{"sku": "SKU-A", "qty": 2, "unitPrice": 150}},
	}

	// Authentication and authorization.
	anon.do(t, http.MethodGet, "/orders/"+customerID, nil, nil).
		expectProblem(t, http.StatusUnauthorized, string(mediator.CodeUnauthorized), "GET without a token")
	anon.do(t, http.MethodPost, "/orders", body, nil).
		expectProblem(t, http.StatusUnauthorized, string(mediator.CodeUnauthorized), "POST without a token")
	reader.do(t, http.MethodPost, "/orders", body, nil).
		expectProblem(t, http.StatusForbidden, string(mediator.CodeForbidden), "POST without orders:write")

	// Idempotent create: the replay returns the first response.
	key := "create-" + randomHex(4)
	first := writer.do(t, http.MethodPost, "/orders", body, map[string]string{"Idempotency-Key": key}).
		expect(t, http.StatusCreated, "create order")
	var created struct {
		OrderID string `json:"orderId"`
	}
	first.decode(t, &created)
	if created.OrderID == "" {
		t.Fatalf("create returned no orderId: %s", first.Body)
	}
	replay := writer.do(t, http.MethodPost, "/orders", body, map[string]string{"Idempotency-Key": key}).
		expect(t, http.StatusCreated, "replayed create")
	if !bytes.Equal(bytes.TrimSpace(replay.Body), bytes.TrimSpace(first.Body)) {
		t.Errorf("replay body %s differs from first %s", replay.Body, first.Body)
	}
	if db.count(t, `SELECT count(*) FROM orders WHERE customer_id = $1`, customerID) != 1 {
		t.Error("the replayed create must not create a second order")
	}
	orderPath := "/orders/" + created.OrderID

	// Cache: miss, hit, then invalidation by AddLine and a hit that shows
	// the new line (G9).
	var view orderView
	writer.do(t, http.MethodGet, orderPath, nil, nil).expect(t, http.StatusOK, "get order").
		expectCache(t, "miss").decode(t, &view)
	if view.Status != "draft" || len(view.Lines) != 1 || view.Total != 300 {
		t.Errorf("order view %+v", view)
	}
	writer.do(t, http.MethodGet, orderPath, nil, nil).expect(t, http.StatusOK, "get order again").
		expectCache(t, "hit").decode(t, &view)
	if len(view.Lines) != 1 {
		t.Errorf("cached view %+v", view)
	}
	writer.do(t, http.MethodPost, orderPath+"/lines", map[string]any{"sku": "SKU-B", "qty": 1, "unitPrice": 500}, nil).
		expect(t, http.StatusNoContent, "add line")
	writer.do(t, http.MethodGet, orderPath, nil, nil).expect(t, http.StatusOK, "get order after add line").
		expectCache(t, "miss").decode(t, &view)
	if len(view.Lines) != 2 || view.Total != 800 {
		t.Errorf("view after add line %+v", view)
	}
	writer.do(t, http.MethodGet, orderPath, nil, nil).expect(t, http.StatusOK, "get order after add line, cached").
		expectCache(t, "hit").decode(t, &view)
	if len(view.Lines) != 2 || view.Total != 800 {
		t.Errorf("cached view after add line is stale: %+v", view)
	}

	// List: cached, then never stale after a create.
	listPath := "/orders?customerId=" + customerID
	var page struct {
		Items []orders.OrderSummary `json:"items"`
		Total int64                 `json:"total"`
	}
	reader.do(t, http.MethodGet, listPath, nil, nil).expect(t, http.StatusOK, "list").expectCache(t, "miss").decode(t, &page)
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].LineCount != 2 {
		t.Errorf("list %+v", page)
	}
	reader.do(t, http.MethodGet, listPath, nil, nil).expect(t, http.StatusOK, "list again").expectCache(t, "hit")
	writer.do(t, http.MethodPost, "/orders", body, map[string]string{"Idempotency-Key": "create-" + randomHex(4)}).
		expect(t, http.StatusCreated, "create second order")
	reader.do(t, http.MethodGet, listPath, nil, nil).expect(t, http.StatusOK, "list after create").
		expectCache(t, "miss").decode(t, &page)
	if page.Total != 2 || len(page.Items) != 2 {
		t.Errorf("list after a create is stale: %+v", page)
	}

	// Submit: the event fans out to both consumers exactly once.
	writer.do(t, http.MethodPost, orderPath+"/submit", nil, nil).expect(t, http.StatusNoContent, "submit")
	writer.do(t, http.MethodPost, orderPath+"/submit", nil, nil).
		expectProblem(t, http.StatusPreconditionFailed, string(mediator.CodePrecondition), "submit twice")
	writer.do(t, http.MethodPost, orderPath+"/lines", map[string]any{"sku": "SKU-A", "qty": 1, "unitPrice": 1}, nil).
		expectProblem(t, http.StatusPreconditionFailed, string(mediator.CodePrecondition), "add line after submit")
	var eventID string
	if err := db.pool.QueryRow(ctx, `SELECT event_id::text FROM mediator_outbox WHERE topic = 'OrderSubmitted' AND stream_key = $1`, created.OrderID).Scan(&eventID); err != nil {
		t.Fatalf("outbox row: %v", err)
	}
	eventually(t, 30*time.Second, "both consumers to apply OrderSubmitted", func() bool {
		return db.count(t, `SELECT count(*) FROM order_summaries WHERE order_id = $1`, created.OrderID) == 1 &&
			db.count(t, `SELECT count(*) FROM reservations WHERE order_id = $1`, created.OrderID) == 2
	})
	if n := db.count(t, `SELECT count(*) FROM order_summaries WHERE order_id = $1 AND status = 'submitted' AND total = 800`, created.OrderID); n != 1 {
		t.Errorf("order_summaries submitted rows: %d", n)
	}
	if n := db.count(t, `SELECT count(*) FROM reservations WHERE order_id = $1 AND status = 'reserved'`, created.OrderID); n != 2 {
		t.Errorf("reserved reservations: %d, want 2", n)
	}
	var a, b int64
	if err := db.pool.QueryRow(ctx, `SELECT (SELECT available FROM inventory WHERE sku = 'SKU-A'), (SELECT available FROM inventory WHERE sku = 'SKU-B')`).Scan(&a, &b); err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if a != 98 || b != 49 {
		t.Errorf("inventory after reservation: SKU-A=%d SKU-B=%d, want 98 and 49", a, b)
	}
	if n := db.count(t, `SELECT count(*) FROM mediator_inbox WHERE event_id = $1`, eventID); n != int64(len(orders.Groups)) {
		t.Errorf("inbox rows for the event: %d, want %d", n, len(orders.Groups))
	}
	if n := db.count(t, `SELECT count(*) FROM audit_log WHERE order_id = $1 AND action = 'order.submitted' AND actor = 'alice'`, created.OrderID); n != 1 {
		t.Errorf("audit rows: %d", n)
	}
	for _, g := range orders.Groups {
		entries, err := redisx.DLQList(ctx, rc, n.rcfg, g)
		if err != nil {
			t.Fatalf("dlq %s: %v", g, err)
		}
		if len(entries) != 0 {
			t.Errorf("dlq %s has %d entries: %+v", g, len(entries), entries)
		}
	}
	eventually(t, 10*time.Second, "no pending entries", func() bool {
		return n.pendingFor(t, rc, orders.GroupReadModel, orderTopics)+n.pendingFor(t, rc, orders.GroupInventory, orderTopics) == 0
	})
	writer.do(t, http.MethodGet, orderPath, nil, nil).expect(t, http.StatusOK, "get submitted order").
		expectCache(t, "miss").decode(t, &view)
	if view.Status != "submitted" || view.SubmittedAt == nil {
		t.Errorf("view after submit %+v", view)
	}

	// SSE: the order's events from the beginning, ending after submitted.
	events, perr := readSSE(t, n.base+orderPath+"/events", writer.token)
	if perr != nil {
		t.Fatalf("stream error frame: %+v", *perr)
	}
	if len(events) == 0 {
		t.Fatal("no SSE events")
	}
	var types []string
	for _, e := range events {
		types = append(types, e.Type)
		if e.ID == "" || e.OrderID != created.OrderID {
			t.Errorf("event %+v lacks an id or has the wrong order", e)
		}
	}
	if want := []string{orders.EventCreated, orders.EventLineAdded, orders.EventSubmitted}; strings.Join(types, ",") != strings.Join(want, ",") {
		t.Errorf("event types %v, want %v", types, want)
	}
	// Resuming after the first event replays only the rest.
	if rest, perr := readSSE(t, n.base+orderPath+"/events", writer.token, "Last-Event-ID", events[0].ID); perr != nil || len(rest) != len(events)-1 {
		t.Errorf("resume after %s returned %d events (error %+v), want %d", events[0].ID, len(rest), perr, len(events)-1)
	}
	// Stream routes are always SSE (design notes 3.5): a rejected stream
	// answers 200 with a single event: error frame carrying the problem.
	if _, perr := readSSE(t, n.base+orderPath+"/events", ""); perr == nil || perr.Code != string(mediator.CodeUnauthorized) || perr.Status != http.StatusUnauthorized {
		t.Errorf("SSE without a token: error frame %+v, want an unauthorized problem", perr)
	}

	n.requestShutdown(t)
	if code := n.waitExit(t, nodeDrainTimeout*3); code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
}

// orderView is the GetOrder response as the test reads it.
type orderView struct {
	OrderID     string             `json:"orderId"`
	CustomerID  string             `json:"customerId"`
	Status      string             `json:"status"`
	Lines       []orders.OrderLine `json:"lines"`
	Total       int64              `json:"total"`
	SubmittedAt *time.Time         `json:"submittedAt"`
}

// expectCache asserts the X-Mediator-Cache header (spec 8.4).
func (r response) expectCache(t *testing.T, want string) response {
	t.Helper()
	if got := r.Header.Get("X-Mediator-Cache"); got != want {
		t.Errorf("X-Mediator-Cache %q, want %q", got, want)
	}
	return r
}

// sseEvent is one data frame of the WatchOrder stream with its id.
type sseEvent struct {
	ID      string
	Type    string `json:"type"`
	OrderID string `json:"orderId"`
}

// readSSE opens the stream with the token ("" for none) and optional header
// pairs and returns every data frame until the server closes it, plus the
// problem of the terminal event: error frame when there was one.
func readSSE(t *testing.T, url, tok string, headers ...string) ([]sseEvent, *problem) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Accept", "text/event-stream")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("stream content type %q", ct)
	}
	var out []sseEvent
	var perr *problem
	var cur sseEvent
	var data, event string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			switch {
			case event == "error":
				perr = &problem{}
				if err := json.Unmarshal([]byte(data), perr, json.DefaultOptionsV2()); err != nil {
					t.Fatalf("stream error frame %q: %v", data, err)
				}
			case data != "":
				if err := json.Unmarshal([]byte(data), &cur); err != nil {
					t.Fatalf("stream item %q: %v", data, err)
				}
				out = append(out, cur)
			}
			cur, data, event = sseEvent{}, "", ""
		case strings.HasPrefix(line, "id: "):
			cur.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data += strings.TrimPrefix(line, "data: ")
		case strings.HasPrefix(line, ":"):
			// keepalive comment
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	return out, perr
}
