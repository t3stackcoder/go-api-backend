package main

import (
	"context"
	"log/slog"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// FencingLogName is the name of the fencing log behavior.
const FencingLogName = "ChaosFencingLog"

// fencingLog is the consumer-path behavior that writes one log record per
// committed consumer apply in the form the chaos checker parses (spec 11.6,
// G14): "fencing=<n> partition=<p> group=<g> node=<id> topic=<t>". It is
// positioned after Inbox, so a duplicate delivery the inbox skipped is not
// logged, and the record is written from an on-commit hook, so an apply
// whose transaction rolled back (for example a stale lease holder cancelled
// at commit) is not logged either.
type fencingLog struct {
	logger *slog.Logger
	node   string
}

func (fencingLog) Name() string { return FencingLogName }

func (b fencingLog) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	res, err := next(ctx, req)
	if err != nil || info.Kind != mediator.KindConsumer {
		return res, err
	}
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return res, nil
	}
	token, _ := mediator.FencingToken(ctx)
	group := info.Group
	record := func(context.Context) {
		b.logger.Info("consumer apply",
			"fencing", token, "partition", env.Partition, "group", group, "node", b.node,
			"topic", env.Topic, "key", env.StreamKey, "seq", env.Seq, "event_id", env.ID.String())
	}
	if _, inTx := pg.TxFrom(ctx); inTx {
		pg.OnCommit(ctx, record)
	} else {
		record(ctx)
	}
	return res, nil
}
