package httpapi

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// EventIDer is implemented by stream items that carry an SSE event ID. The
// adapter writes it as the id: line of the frame, which browsers send back
// as Last-Event-ID on reconnect; a request field tagged
// header:"Last-Event-ID" receives it, which is how resumable streams are
// built.
type EventIDer interface{ EventID() string }

// streamItem is one element of the handler's sequence.
type streamItem struct {
	v   any
	err error
}

// serveStream serves a stream request as Server-Sent Events: headers are
// flushed at once, every item is one data: frame, a terminal error is an
// event: error frame with a problem body, a ": keepalive" comment is written
// every Config.KeepAlive, the client disconnecting cancels the handler, and
// a Listener shutdown ends the stream with a CodeUnavailable error frame.
func (s *Server) serveStream(w http.ResponseWriter, r *http.Request, req any) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	h.Del("Content-Length")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		s.logger.Warn("sse: cannot flush response", "error", err, "correlation_id", mediator.CorrelationID(ctx))
		return
	}

	items := make(chan streamItem)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v, err := range s.m.StreamAny(ctx, req) {
			select {
			case items <- streamItem{v: v, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	keepalive := time.NewTicker(s.cfg.KeepAlive)
	defer keepalive.Stop()
	draining := drainSignal(ctx)
	for {
		select {
		case it := <-items:
			if it.err != nil {
				s.writeErrorFrame(ctx, w, rc, r, it.err)
				return
			}
			frame, err := dataFrame(it.v)
			if err != nil {
				s.writeErrorFrame(ctx, w, rc, r, mediator.Wrap(mediator.CodeInternal, "encode stream item", err))
				return
			}
			if err := sseWrite(ctx, w, rc, frame); err != nil {
				return
			}
		case <-done:
			return
		case <-keepalive.C:
			if err := sseWrite(ctx, w, rc, ": keepalive\n\n"); err != nil {
				return
			}
		case <-ctx.Done():
			return
		case <-draining:
			s.writeErrorFrame(ctx, w, rc, r, mediator.E(mediator.CodeUnavailable, "server is shutting down"))
			return
		}
	}
}

// dataFrame encodes one item as an SSE frame, with an id: line when the item
// implements EventIDer.
func dataFrame(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if e, ok := v.(EventIDer); ok {
		sb.WriteString("id: ")
		sb.WriteString(strings.NewReplacer("\r", "", "\n", "").Replace(e.EventID()))
		sb.WriteString("\n")
	}
	sb.WriteString("data: ")
	sb.Write(b)
	sb.WriteString("\n\n")
	return sb.String(), nil
}

// writeErrorFrame writes the terminal event: error frame. Internal errors
// are logged exactly as WriteProblem logs them.
func (s *Server) writeErrorFrame(ctx context.Context, w io.Writer, rc *http.ResponseController, r *http.Request, err error) {
	p := ProblemOf(err, r.URL.Path, mediator.CorrelationID(ctx))
	logProblem(s.logger, p, err)
	_ = sseWrite(ctx, w, rc, "event: error\ndata: "+string(marshalProblem(p))+"\n\n")
}

// sseWrite writes one frame and flushes it. Fault point http.sse.write.
func sseWrite(ctx context.Context, w io.Writer, rc *http.ResponseController, frame string) error {
	if err := testkit.Fault(ctx, "http.sse.write"); err != nil {
		return err
	}
	if _, err := io.WriteString(w, frame); err != nil {
		return err
	}
	return rc.Flush()
}
