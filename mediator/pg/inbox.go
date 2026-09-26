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
