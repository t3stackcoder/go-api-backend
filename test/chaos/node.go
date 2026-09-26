//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// node is one chaosnode container: its workload HTTP endpoint, its
// container name, and the Toxiproxy proxies it reaches Postgres and Redis
// through (deploy/toxiproxy.json: pg-nodeN, redis-nodeN).
type node struct {
	Index      int
	ID         string
	Addr       string
	Container  string
	PGProxy    string
	RedisProxy string

	http  *http.Client // workload requests, bounded by the request timeout
	admin *http.Client // admin endpoints under /chaos/
}

func newNode(i int, addr, prefix string, timeout time.Duration) *node {
	id := fmt.Sprintf("node%d", i+1)
	tr := &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 64, IdleConnTimeout: 60 * time.Second, DisableCompression: true}
	return &node{
		Index: i, ID: id, Addr: strings.TrimRight(addr, "/"),
		Container: prefix + "-" + id + "-1", PGProxy: "pg-" + id, RedisProxy: "redis-" + id,
		http:  &http.Client{Transport: tr, Timeout: timeout},
		admin: &http.Client{Timeout: 8 * time.Second},
	}
}

// outcome classifies one workload request in Jepsen terms (Appendix C).
type outcome int

const (
	// outcomeOK: the operation definitely happened.
	outcomeOK outcome = iota
	// outcomeFail: the operation definitely did not happen.
	outcomeFail
	// outcomeInfo: unknown; the process is retired.
	outcomeInfo
)

func (o outcome) String() string {
	switch o {
	case outcomeOK:
		return "ok"
	case outcomeFail:
		return "fail"
	default:
		return "info"
	}
}

// result is the classified response of one workload request.
type result struct {
	Outcome outcome
	Status  int
	Code    string // problem code when the node answered with a problem
	Err     string // recorded as Op.Error for fail and info
	Body    []byte
	Cache   string // X-Mediator-Cache
	Dial    bool   // the connection could not be made: nothing was sent
}

// request is one workload HTTP call.
type request struct {
	Method string
	Path   string
	Query  url.Values
	Body   any
}

// definiteCodes are the problem codes that mean the request was rejected
// before or instead of running its handler's effects, so the operation
// definitely did not happen (fail). Everything else (timeout, unavailable,
// internal, idempotency_in_progress, an unreadable answer) is info.
var definiteCodes = map[string]bool{
	"bad_request": true, "payload_too_large": true, "unsupported_media_type": true,
	"validation": true, "not_found": true, "conflict": true, "precondition_failed": true,
	"unauthorized": true, "forbidden": true, "rate_limited": true, "idempotency_mismatch": true,
	"handler_not_found": true, "method_not_allowed": true,
}

type problem struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// call performs one workload request and classifies the answer.
func (n *node) call(ctx context.Context, req request) result {
	var body io.Reader
	if req.Body != nil {
		b, err := json.Marshal(req.Body)
		if err != nil {
			return result{Outcome: outcomeFail, Err: "encode: " + err.Error()}
		}
		body = bytes.NewReader(b)
	}
	u := n.Addr + req.Path
	if len(req.Query) > 0 {
		u += "?" + req.Query.Encode()
	}
	hr, err := http.NewRequestWithContext(ctx, req.Method, u, body)
	if err != nil {
		return result{Outcome: outcomeFail, Err: "request: " + err.Error()}
	}
	if body != nil {
		hr.Header.Set("Content-Type", "application/json")
	}
	resp, err := n.http.Do(hr)
	if err != nil {
		return classifyTransport(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return result{Outcome: outcomeInfo, Status: resp.StatusCode, Err: "read body: " + shortErr(err)}
	}
	res := result{Status: resp.StatusCode, Body: raw, Cache: resp.Header.Get("X-Mediator-Cache")}
	if resp.StatusCode/100 == 2 {
		res.Outcome = outcomeOK
		return res
	}
	var p problem
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "json") {
		_ = json.Unmarshal(raw, &p)
	}
	res.Code = p.Code
	if res.Code == "" {
		res.Code = fmt.Sprintf("http_%d", resp.StatusCode)
	}
	res.Err = res.Code
	switch {
	case definiteCodes[p.Code], p.Code == "" && resp.StatusCode/100 == 4:
		res.Outcome = outcomeFail
	default:
		res.Outcome = outcomeInfo
	}
	return res
}

// classifyTransport maps a transport error: a dial failure means nothing
// reached a node (fail); a timeout or a broken connection after the request
// was sent leaves the outcome unknown (info).
func classifyTransport(err error) result {
	var ne *net.OpError
	if errors.As(err, &ne) && ne.Op == "dial" {
		return result{Outcome: outcomeFail, Err: "dial: " + shortErr(err), Dial: true}
	}
	var ue *url.Error
	if (errors.As(err, &ue) && ue.Timeout()) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return result{Outcome: outcomeInfo, Err: "client_timeout"}
	}
	if errors.Is(err, context.Canceled) {
		return result{Outcome: outcomeInfo, Err: "canceled"}
	}
	return result{Outcome: outcomeInfo, Err: "transport: " + shortErr(err)}
}

func shortErr(err error) string {
	s := err.Error()
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// adminPut calls an admin endpoint with query parameters.
func (n *node) adminPut(ctx context.Context, path string, q url.Values) error {
	hr, err := http.NewRequestWithContext(ctx, http.MethodPut, n.Addr+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := n.admin.Do(hr)
	if err != nil {
		return fmt.Errorf("%s: PUT %s: %w", n.ID, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: PUT %s: status %d: %s", n.ID, path, resp.StatusCode, bytes.TrimSpace(raw))
	}
	return nil
}

// adminGet calls an admin endpoint and decodes its JSON answer into v.
func (n *node) adminGet(ctx context.Context, path string, v any) error {
	hr, err := http.NewRequestWithContext(ctx, http.MethodGet, n.Addr+path, nil)
	if err != nil {
		return err
	}
	resp, err := n.admin.Do(hr)
	if err != nil {
		return fmt.Errorf("%s: GET %s: %w", n.ID, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: GET %s: status %d: %s", n.ID, path, resp.StatusCode, bytes.TrimSpace(raw))
	}
	return json.Unmarshal(raw, v)
}

// setClock shifts the node's injectable clock (clock-skew nemesis).
func (n *node) setClock(ctx context.Context, d time.Duration) error {
	return n.adminPut(ctx, "/chaos/clock", url.Values{"offset": {d.String()}})
}

// setRelay stops or starts the node's relay (relay-kill nemesis).
func (n *node) setRelay(ctx context.Context, run bool) error {
	return n.adminPut(ctx, "/chaos/relay", url.Values{"run": {strconv.FormatBool(run)}})
}

// setHandlers sets the dispatch mode of the workload routes and whether the
// node serves remote commands (remote-send workload, handler-rotate).
func (n *node) setHandlers(ctx context.Context, dispatch string, serve bool) error {
	return n.adminPut(ctx, "/chaos/handlers", url.Values{"dispatch": {dispatch}, "serve": {strconv.FormatBool(serve)}})
}

// goroutines reads /chaos/goroutines.
func (n *node) goroutines(ctx context.Context) (int, error) {
	var v struct {
		Goroutines int `json:"goroutines"`
	}
	if err := n.adminGet(ctx, "/chaos/goroutines", &v); err != nil {
		return 0, err
	}
	return v.Goroutines, nil
}

// stats reads /chaos/stats.
func (n *node) stats(ctx context.Context) (map[string]any, error) {
	var v map[string]any
	if err := n.adminGet(ctx, "/chaos/stats", &v); err != nil {
		return nil, err
	}
	return v, nil
}

// ready reports whether /readyz answers 200.
func (n *node) ready(ctx context.Context) bool {
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	hr, err := http.NewRequestWithContext(rctx, http.MethodGet, n.Addr+"/readyz", nil)
	if err != nil {
		return false
	}
	resp, err := n.admin.Do(hr)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

// waitReady polls readiness until it holds or the timeout passes.
func (n *node) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if n.ready(ctx) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: not ready within %s", n.ID, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
