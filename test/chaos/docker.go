//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// dockerCLI drives containers through the docker command line: pause,
// unpause, kill with a signal, start, stop, restart, wait, inspect, logs,
// exec, and cp (spec 11.6, nemeses table). Every call is stateless, so the
// nemesis goroutines share one value.
type dockerCLI struct {
	log *slog.Logger
}

// run executes docker with args and returns its stdout.
func (d dockerCLI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	started := time.Now()
	err := cmd.Run()
	if d.log != nil {
		d.log.Debug("docker", "args", strings.Join(args, " "), "elapsed", time.Since(started).String(), "error", err)
	}
	if err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

func (d dockerCLI) pause(ctx context.Context, c string) error {
	_, err := d.run(ctx, "pause", c)
	return err
}

func (d dockerCLI) unpause(ctx context.Context, c string) error {
	_, err := d.run(ctx, "unpause", c)
	return err
}

// kill sends a signal (KILL, TERM, INT) to the container's main process.
func (d dockerCLI) kill(ctx context.Context, c, signal string) error {
	_, err := d.run(ctx, "kill", "-s", signal, c)
	return err
}

func (d dockerCLI) start(ctx context.Context, c string) error {
	_, err := d.run(ctx, "start", c)
	return err
}

// stop stops the containers with their STOPSIGNAL, killing them after timeout.
func (d dockerCLI) stop(ctx context.Context, timeout time.Duration, containers ...string) error {
	_, err := d.run(ctx, append([]string{"stop", "-t", strconv.Itoa(int(timeout.Seconds()))}, containers...)...)
	return err
}

// restart restarts the container with its STOPSIGNAL and a stop timeout.
func (d dockerCLI) restart(ctx context.Context, c string, timeout time.Duration) error {
	_, err := d.run(ctx, "restart", "-t", strconv.Itoa(int(timeout.Seconds())), c)
	return err
}

// wait blocks until the container exits, up to timeout, and returns its
// exit code. exited is false when the timeout passed first.
func (d dockerCLI) wait(ctx context.Context, c string, timeout time.Duration) (code int, exited bool, err error) {
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := d.run(wctx, "wait", c)
	if err != nil {
		if wctx.Err() != nil && ctx.Err() == nil {
			return 0, false, nil
		}
		return 0, false, err
	}
	code, err = strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0, false, fmt.Errorf("docker wait %s: unexpected output %q", c, out)
	}
	return code, true, nil
}

// containerState is the part of docker inspect the harness reads.
type containerState struct {
	ExitCode int
	Status   string
	Paused   bool
	Running  bool
}

// state inspects the container.
func (d dockerCLI) state(ctx context.Context, c string) (containerState, error) {
	out, err := d.run(ctx, "inspect", "-f", "{{.State.ExitCode}} {{.State.Status}} {{.State.Paused}} {{.State.Running}}", c)
	if err != nil {
		return containerState{}, err
	}
	f := strings.Fields(out)
	if len(f) != 4 {
		return containerState{}, fmt.Errorf("docker inspect %s: unexpected output %q", c, out)
	}
	var s containerState
	s.ExitCode, _ = strconv.Atoi(f[0])
	s.Status = f[1]
	s.Paused = f[2] == "true"
	s.Running = f[3] == "true"
	return s, nil
}

// logs returns the container's stdout and stderr since the given time,
// which spans every restart of the container within the run but not the
// earlier runs of the same container (the compose services are reused).
func (d dockerCLI) logs(ctx context.Context, c string, since time.Time) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "--since", since.UTC().Format(time.RFC3339), c) //nolint:gosec // container name and timestamp come from the harness
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		return buf.String(), fmt.Errorf("docker logs %s: %w", c, err)
	}
	return buf.String(), nil
}

// exec runs a command inside a running container.
func (d dockerCLI) exec(ctx context.Context, c string, args ...string) (string, error) {
	return d.run(ctx, append([]string{"exec", c}, args...)...)
}

// cp copies between the host and a container in either direction; a
// stopped container is fine.
func (d dockerCLI) cp(ctx context.Context, src, dst string) error {
	_, err := d.run(ctx, "cp", src, dst)
	return err
}

// ensureRunning unpauses a paused container and starts a stopped one.
func (d dockerCLI) ensureRunning(ctx context.Context, c string) error {
	st, err := d.state(ctx, c)
	if err != nil {
		return err
	}
	if st.Paused {
		if err := d.unpause(ctx, c); err != nil {
			return err
		}
	}
	if !st.Running {
		if err := d.start(ctx, c); err != nil {
			return err
		}
	}
	return nil
}
