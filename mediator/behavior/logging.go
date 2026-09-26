package behavior

import (
	"context"
	"iter"
	"log/slog"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// NewLogging returns the Logging behavior (5.4): request.start and
// request.end records at info level with name, kind, correlation_id,
// request_id, and, at the end, duration_ms, outcome, and error_code. With
// Config.LogPayloads the start record carries the request under "request"
// after redaction: fields tagged log:"-" or log:"redact" are replaced by
// "[redacted]" at any depth. A stream logs its item count and terminal
// error when the sequence ends.
func NewLogging(cfg Config) mediator.Behavior {
	return &logging{logger: cfg.logger(), clock: cfg.clock(), payloads: cfg.LogPayloads, red: &redactor{}}
}

type logging struct {
	logger   *slog.Logger
	clock    mediator.Clock
	payloads bool
	red      *redactor
}

func (l *logging) Name() string { return Logging }

func (l *logging) enabled(ctx context.Context) bool { return l.logger.Enabled(ctx, slog.LevelInfo) }

func (l *logging) start(ctx context.Context, req any, info *mediator.RequestInfo) {
	attrs := make([]slog.Attr, 0, 6)
	attrs = append(attrs,
		slog.String("name", info.Name),
		slog.String("kind", info.Kind.String()),
		slog.String("correlation_id", mediator.CorrelationID(ctx)),
		slog.String("request_id", mediator.RequestID(ctx).String()))
	if info.Group != "" {
		attrs = append(attrs, slog.String("group", info.Group))
	}
	if l.payloads {
		attrs = append(attrs, slog.Any("request", l.red.Redact(req)))
	}
	l.logger.LogAttrs(ctx, slog.LevelInfo, "request.start", attrs...)
}

// end writes request.end. items is -1 for non-stream calls.
func (l *logging) end(ctx context.Context, info *mediator.RequestInfo, start time.Time, o outcome, err error, items int) {
	attrs := make([]slog.Attr, 0, 10)
	attrs = append(attrs,
		slog.String("name", info.Name),
		slog.String("kind", info.Kind.String()),
		slog.String("correlation_id", mediator.CorrelationID(ctx)),
		slog.String("request_id", mediator.RequestID(ctx).String()),
		slog.Float64("duration_ms", float64(l.clock.Now().Sub(start))/float64(time.Millisecond)),
		slog.String("outcome", o.String()))
	if info.Group != "" {
		attrs = append(attrs, slog.String("group", info.Group))
	}
	switch {
	case o == outcomePanic && err == nil:
		attrs = append(attrs, slog.String("error_code", string(mediator.CodeInternal)))
	case err != nil:
		attrs = append(attrs, slog.String("error_code", string(mediator.CodeOf(err))), slog.String("error", err.Error()))
	default:
		attrs = append(attrs, slog.String("error_code", ""))
	}
	if items >= 0 {
		attrs = append(attrs, slog.Int("items", items))
	}
	l.logger.LogAttrs(ctx, slog.LevelInfo, "request.end", attrs...)
}

func (l *logging) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (res any, err error) {
	if !l.enabled(ctx) {
		return next(ctx, req)
	}
	start := l.clock.Now()
	l.start(ctx, req, info)
	done := false
	defer func() {
		if !done {
			l.end(ctx, info, start, outcomePanic, nil, -1)
		}
	}()
	res, err = next(ctx, req)
	done = true
	l.end(ctx, info, start, outcomeOf(err), err, -1)
	return res, err
}

func (l *logging) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	if !l.enabled(ctx) {
		return next(ctx, req)
	}
	return func(yield func(any, error) bool) {
		start := l.clock.Now()
		l.start(ctx, req, info)
		items := 0
		done := false
		defer func() {
			if !done {
				l.end(ctx, info, start, outcomePanic, nil, items)
			}
		}()
		for v, err := range next(ctx, req) {
			if err != nil {
				done = true
				l.end(ctx, info, start, outcomeOf(err), err, items)
				yield(nil, err)
				return
			}
			items++
			if !yield(v, nil) {
				done = true
				l.end(ctx, info, start, outcomeOK, nil, items)
				return
			}
		}
		done = true
		l.end(ctx, info, start, outcomeOK, nil, items)
	}
}
