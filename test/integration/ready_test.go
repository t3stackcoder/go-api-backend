//go:build integration

package integration

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestReady_ReflectsRedisDown is spec 9.2: /readyz is 200 while Redis
// answers PING, 503 with a body naming redis while it is stopped, and 200
// again once it is back. The test owns a Redis container bound to a fixed
// host port, because Docker may map a restarted container to a different
// ephemeral port and the node keeps the address it started with; the
// shared container of the package keeps serving the other tests.
func TestReady_ReflectsRedisDown(t *testing.T) {
	ctx := context.Background()
	port := freePort(t)
	rd, err := testcontainers.Run(ctx, "redis:8",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("6379/tcp"): {{HostPort: strconv.Itoa(port)}}}
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(30*time.Second),
			wait.ForLog("Ready to accept connections"),
		))
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(rd) })
	addr := "127.0.0.1:" + strconv.Itoa(port)

	db := newDatabase(t)
	n := startNode(t, nodeConfig{db: db, redisAddr: addr})
	n.waitReady(t, 60*time.Second)
	c := n.client("")
	c.http.Timeout = 10 * time.Second

	c.do(t, http.MethodGet, "/readyz", nil, nil).expect(t, http.StatusOK, "readyz with redis up")

	stopTimeout := 5 * time.Second
	if err := rd.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	var down response
	eventually(t, 30*time.Second, "readyz to report 503", func() bool {
		down = c.do(t, http.MethodGet, "/readyz", nil, nil)
		return down.Status == http.StatusServiceUnavailable
	})
	p := down.expectProblem(t, http.StatusServiceUnavailable, "unavailable", "readyz with redis down")
	if !strings.Contains(strings.ToLower(string(down.Body)), "redis") {
		t.Errorf("readyz body does not name redis: %s", down.Body)
	}
	t.Logf("readyz while redis is down: %s (%v)", p.Detail, p.Details["failures"])
	c.do(t, http.MethodGet, "/healthz", nil, nil).expect(t, http.StatusOK, "healthz stays 200 while redis is down")
	select {
	case <-n.done:
		t.Fatalf("node exited while redis was down: %v", n.waitErr)
	default:
	}

	if err := rd.Start(ctx); err != nil {
		t.Fatalf("restart redis: %v", err)
	}
	// Components recover on their next heartbeat (5 s lease renew, 5 s
	// remote heartbeat); a janitor sweep that failed during the outage is
	// cleared by the next sweep a minute later, hence the bound.
	var back response
	eventually(t, 90*time.Second, "readyz to return 200 after redis restarted", func() bool {
		back = c.do(t, http.MethodGet, "/readyz", nil, nil)
		return back.Status == http.StatusOK
	})
	t.Logf("readyz after restart: %s", back)

	n.requestShutdown(t)
	if code := n.waitExit(t, nodeDrainTimeout*3); code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
}
