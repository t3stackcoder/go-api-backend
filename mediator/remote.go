package mediator

import "context"

// RemoteDispatcher sends a request to another node. redisx.Remote implements
// it over Redis Streams. Send consults it for request types registered with
// Declare, or whose local handler is absent.
type RemoteDispatcher interface {
	// Send dispatches req to a node serving info.Name and returns the decoded
	// response of type info.ResponseType. It returns ErrHandlerNotFound when
	// no node has a fresh heartbeat for the request.
	Send(ctx context.Context, info *RequestInfo, req any) (any, error)
}
