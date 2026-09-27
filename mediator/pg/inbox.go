package pg

import (
	"context"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Inbox returns the behavior of 6.5 (name mediator.NameInbox) for the
// consumer path. Inside the ambient unit of work it records
// (group, event ID) in mediator_inbox before the handler runs. When the row
// already exists the handler is skipped, ConsumerState.Duplicate is set,
// and the empty transaction commits so the delivery can be acknowledged.
// A concurrent redelivery blocks on the row lock until the first attempt
// ends, then skips or proceeds.
//
// When the context carries a fencing token (mediator.FencingToken, set by
// the consumer transport from its partition lease) the behavior first
// records it in mediator_partition_epoch through Tx.FencePartition (7.2,
// G14). A token below the stored epoch means a newer owner has applied to
// the partition since: the delivery fails with CodeConflict wrapping
// ErrStaleLease before the inbox insert, nothing is committed, and the
// transport is expected to end the lease rather than retry.
func Inbox() mediator.Behavior { return inboxBehavior{} }

type inboxBehavior struct{}

func (inboxBehavior) Name() string { return mediator.NameInbox }

func (inboxBehavior) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	if info.Kind != mediator.KindConsumer {
		return next(ctx, req)
	}
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return nil, mediator.E(mediator.CodeInternal, "inbox: no envelope in context")
	}
	tx, ok := StoreTxFrom(ctx)
	if !ok {
		return nil, mediator.Wrap(mediator.CodeInternal, "inbox: the unit of work must run before the inbox", ErrNoUnitOfWork)
	}
	if token, ok := mediator.FencingToken(ctx); ok {
		fenced, err := tx.FencePartition(ctx, info.Group, env.Topic, env.Partition, token)
		if err != nil {
			if IsLockTimeout(err) {
				return nil, mediator.Wrap(mediator.CodeUnavailable, "inbox: the partition epoch is held by another consumer", err)
			}
			return nil, err
		}
		if !fenced {
			return nil, mediator.Wrap(mediator.CodeConflict, "inbox: stale lease; a newer owner has taken the partition", ErrStaleLease)
		}
	}
	fresh, err := tx.InboxInsert(ctx, info.Group, env.ID)
	if err != nil {
		if IsLockTimeout(err) {
			return nil, mediator.Wrap(mediator.CodeUnavailable, "inbox: event is being processed by another consumer", err)
		}
		return nil, err
	}
	if !fresh {
		if st, ok := mediator.ConsumerStateFrom(ctx); ok {
			st.Duplicate = true
		}
		return nil, nil
	}
	return next(ctx, req)
}
