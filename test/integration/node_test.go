//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// nodePartitions mirrors the partitions constant of examples/orders/main.go;
// the tests need it to address the node's streams and leases.
const nodePartitions = 4

// nodeDrainTimeout mirrors drainTimeout of examples/orders/main.go: the
// bound within which a node must exit after a shutdown request (G16).
const nodeDrainTimeout = 15 * time.Second

// nodeConfig describes one orders process to start.
type nodeConfig struct {
	// db is the database the node migrates and uses.
	db *database
	// redisAddr overrides the shared Redis (the readiness test has its own).
	redisAddr string
	// prefix is the Redis key prefix; a fresh one when empty.
	prefix string
	// nodeID names the process; generated when empty.
	nodeID string
	// env holds extra variables such as ORDERS_DEBUG=1.
	env map[string]string
}

// node is a running orders process started by startNode.
type node struct {
	id     string
	prefix string
	// base is the HTTP origin, http://127.0.0.1:port.
	base string
	// rcfg addresses the node's Redis structures (prefix, partitions).
	rcfg redisx.Config

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	logPath string
	done    chan struct{}
	waitErr error
}

// startNode builds the environment of cfg, starts the binary with a stdin
// pipe and stderr in a log file, and registers a cleanup that kills it if
// it still runs when the test ends.
func startNode(t *testing.T, cfg nodeConfig) *node {
	t.Helper()
	n := &node{id: cfg.nodeID, prefix: cfg.prefix, done: make(chan struct{})}
	if n.id == "" {
		n.id = "node-" + randomHex(3)
	}
	if n.prefix == "" {
		n.prefix = uniquePrefix()
	}
	addr := cfg.redisAddr
	if addr == "" {
		addr = redisAddr
	}
	n.rcfg = redisx.Config{Addr: addr, Prefix: n.prefix, NodeID: n.id, PartitionsPerTopic: nodePartitions}.WithDefaults()
	port := freePort(t)
	n.base = "http://127.0.0.1:" + strconv.Itoa(port)

	n.logPath = filepath.Join(t.TempDir(), n.id+".log")
	logFile, err := os.Create(n.logPath)
	if err != nil {
		t.Fatalf("node log: %v", err)
	}
	cmd := exec.Command(ordersBin)
	env := map[string]string{
		"PG_URL":                cfg.db.url,
		"REDIS_ADDR":            addr,
		"REDIS_PREFIX":          n.prefix,
		"HTTP_ADDR":             "127.0.0.1:" + strconv.Itoa(port),
		"NODE_ID":               n.id,
		"JWT_SECRET":            jwtSecret,
		"SHUTDOWN_ON_STDIN_EOF": "1",
	}
	for k, v := range cfg.env {
		env[k] = v
	}
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	n.stdin = stdin
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", ordersBin, err)
	}
	n.cmd = cmd
	go func() {
		n.waitErr = cmd.Wait()
		_ = logFile.Close()
		close(n.done)
	}()
	t.Cleanup(func() {
		select {
		case <-n.done:
		default:
			_ = cmd.Process.Kill()
			<-n.done
		}
		if t.Failed() {
			n.dumpLog(t)
		}
	})
	return n
}

// dumpLog writes the tail of the node's log to the test output.
func (n *node) dumpLog(t *testing.T) {
	t.Helper()
	b, err := os.ReadFile(n.logPath)
	if err != nil {
		t.Logf("node %s: no log: %v", n.id, err)
		return
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) > 60 {
		lines = lines[len(lines)-60:]
	}
	t.Logf("node %s log (last %d lines):\n%s", n.id, len(lines), strings.Join(lines, "\n"))
}

// waitReady blocks until GET /readyz answers 200, failing the test when the
// process exits or the timeout passes first.
func (n *node) waitReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		select {
		case <-n.done:
			t.Fatalf("node %s exited before becoming ready: %v", n.id, n.waitErr)
		default:
		}
		resp, err := client.Get(n.base + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s not ready after %s (last: %v)", n.id, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// requestShutdown asks the node to stop the way an orchestrator would:
// SIGTERM, except on Windows, which cannot signal a child, where closing
// stdin triggers the same graceful shutdown (SHUTDOWN_ON_STDIN_EOF=1).
func (n *node) requestShutdown(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		if err := n.stdin.Close(); err != nil {
			t.Fatalf("close stdin: %v", err)
		}
		return
	}
	if err := n.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
}

// waitExit waits for the process to exit and returns its exit code, failing
// the test when it is still running after timeout.
func (n *node) waitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-n.done:
	case <-time.After(timeout):
		t.Fatalf("node %s still running %s after the shutdown request", n.id, timeout)
	}
	return n.cmd.ProcessState.ExitCode()
}

// redisClient connects to the node's Redis and closes the client at cleanup.
func (n *node) redisClient(t *testing.T) *redis.Client {
	t.Helper()
	client, err := redisx.NewClient(context.Background(), n.rcfg)
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// pendingFor returns the number of delivered, unacknowledged entries of a
// group over the topics on the node's streams.
func (n *node) pendingFor(t *testing.T, client *redis.Client, group string, topics []string) int64 {
	t.Helper()
	lags, err := redisx.ConsumerLag(context.Background(), client, n.rcfg, []string{group}, topics)
	if err != nil {
		t.Fatalf("consumer lag: %v", err)
	}
	var pending int64
	for _, l := range lags {
		pending += l.Pending
	}
	return pending
}

// leasesHeldBy returns the leases of the groups whose value names this node.
func (n *node) leasesHeldBy(t *testing.T, client *redis.Client, groups []string) []redisx.LeaseInfo {
	t.Helper()
	var held []redisx.LeaseInfo
	for _, g := range groups {
		leases, err := redisx.LeaseList(context.Background(), client, n.rcfg, g)
		if err != nil {
			t.Fatalf("lease list %s: %v", g, err)
		}
		for _, l := range leases {
			if l.Node == n.id {
				held = append(held, l)
			}
		}
	}
	return held
}

// freePort asks the kernel for an unused TCP port on the loopback interface.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// client is an HTTP client bound to one node with a bearer token.
type client struct {
	base  string
	token string
	http  *http.Client
}

// client returns an HTTP client for the node with the token ("" for none).
func (n *node) client(token string) *client {
	return &client{base: n.base, token: token, http: &http.Client{Timeout: 30 * time.Second}}
}

// response is a completed HTTP exchange.
type response struct {
	Status int
	Header http.Header
	Body   []byte
}

// do sends method path with body encoded as JSON (nil for no body) and
// the extra headers, and reads the whole response.
func (c *client) do(t *testing.T, method, path string, body any, headers map[string]string) response {
	t.Helper()
	resp, err := c.try(method, path, body, headers)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

// try is do without failing the test on a transport error.
func (c *client) try(method, path string, body any, headers map[string]string) (response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return response{}, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		return response{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{Status: resp.StatusCode, Header: resp.Header, Body: b}, nil
}

// decode unmarshals the body into v, failing the test on a mismatch.
func (r response) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("decode %q: %v", r.Body, err)
	}
}

// expect fails the test unless the status matches, showing the body.
func (r response) expect(t *testing.T, status int, what string) response {
	t.Helper()
	if r.Status != status {
		t.Fatalf("%s: status %d, want %d; body %s", what, r.Status, status, r.Body)
	}
	return r
}

// problem is the RFC 9457 body the adapter writes for errors (spec 8.3).
type problem struct {
	Type    string         `json:"type"`
	Title   string         `json:"title"`
	Status  int            `json:"status"`
	Detail  string         `json:"detail"`
	Code    string         `json:"code"`
	Details map[string]any `json:"details"`
}

// expectProblem asserts a problem+json response with the status and code.
func (r response) expectProblem(t *testing.T, status int, code, what string) problem {
	t.Helper()
	r.expect(t, status, what)
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Errorf("%s: content type %q, want application/problem+json", what, ct)
	}
	var p problem
	if err := json.Unmarshal(r.Body, &p, json.DefaultOptionsV2()); err != nil {
		t.Fatalf("%s: problem body %q: %v", what, r.Body, err)
	}
	if p.Code != code || p.Status != status {
		t.Errorf("%s: problem %+v, want code %s status %d", what, p, code, status)
	}
	return p
}

// String formats a response for messages.
func (r response) String() string { return fmt.Sprintf("%d %s", r.Status, r.Body) }
