package orders

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// Version is the API version written into the OpenAPI document.
const Version = "1.0.0"

// DefaultWatchPoll is the WatchOrder poll interval when Deps names none.
const DefaultWatchPoll = 500 * time.Millisecond

// Deps is what NewMediator and RegisterWith wire the service to. Every field
// is optional: a nil Store, Cache, or Limiter omits the behaviors that need
// it (that is how Registry builds a mediator with no infrastructure), and a
// nil Querier makes WatchOrder answer with an internal error.
type Deps struct {
	// Store is the database behind UnitOfWork, Idempotency, and Inbox.
	Store pg.Store
	// Querier is the read side of WatchOrder, which tails outside any unit
	// of work. Pass the pgxpool.Pool.
	Querier Querier
	// Cache is the tag-versioned cache behind Cache and CacheInvalidation.
	Cache behavior.CacheBackend
	// Limiter is the GCRA limiter behind RateLimit.
	Limiter behavior.RateLimiter
	// Remote enables remote dispatch for request types with no local handler.
	Remote mediator.RemoteDispatcher
	// Logger, Tracer, and Meter feed the standard behaviors. Nil means the
	// slog default and the global OpenTelemetry providers.
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter
	// NodeID names this process in logs, leases, and remote dispatch.
	NodeID string
	// WatchPoll is the WatchOrder poll interval. Default DefaultWatchPoll.
	WatchPoll time.Duration
	// ProjectorDelay makes the read-model projector wait before writing, so
	// tests can stop a node mid-consumption. Zero in production.
	ProjectorDelay time.Duration
	// Debug also registers the debug requests (RegisterDebug). They are not
	// part of the committed OpenAPI document.
	Debug bool
}

// Register registers every request, event, in-process handler, and
// consumer of the service on m with default options. It must run before
// Build. RegisterWith takes the options.
func Register(m *mediator.Mediator) error { return RegisterWith(m, Deps{}) }

// RegisterWith is Register with the handler options of deps (Querier,
// WatchPoll, ProjectorDelay); the behavior fields are ignored here.
func RegisterWith(m *mediator.Mediator, deps Deps) error {
	if m == nil {
		return errors.New("orders: nil mediator")
	}
	s := &service{m: m, querier: deps.Querier, poll: deps.WatchPoll, delay: deps.ProjectorDelay, nodeID: deps.NodeID}
	if s.poll <= 0 {
		s.poll = DefaultWatchPoll
	}
	return errors.Join(
		mediator.HandleFunc(m, s.createOrder),
		mediator.HandleFunc(m, s.addLine),
		mediator.HandleFunc(m, s.submitOrder),
		mediator.HandleFunc(m, s.getOrder),
		mediator.HandleFunc(m, s.listOrders),
		mediator.HandleStreamFunc(m, s.watchOrder),
		mediator.HandleFunc(m, s.reserveStock),
		mediator.OnFunc(m, s.auditSubmitted),
		mediator.ConsumeFunc(m, GroupInventory, s.reserveInventory),
		mediator.ConsumeFunc(m, GroupReadModel, s.projectSummary),
	)
}

// Sleep is a debug query that waits for the given number of milliseconds
// and answers. Integration tests use it as a long in-flight request to
// prove that shutdown drains (spec G16). It is registered only by
// RegisterDebug and so never appears in api/openapi.json.
type Sleep struct {
	mediator.Query[SleepResult]

	Millis int `json:"ms" query:"ms" validate:"min=0,max=60000" doc:"Milliseconds to wait before answering"`
}

// SleepResult is the response of Sleep.
type SleepResult struct {
	SleptMillis int    `json:"sleptMs"`
	Node        string `json:"node"`
}

// Name pins the persisted name under the debug namespace.
func (Sleep) Name() string { return "debug.Sleep" }

// Route is GET /debug/sleep?ms=.
func (Sleep) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodGet, Path: "/debug/sleep"}
}

// Requires an authenticated caller, like every request of the service.
func (Sleep) Requires() authz.Requirement { return authz.Authenticated() }

// NoUnitOfWork: the query touches no data.
func (Sleep) NoUnitOfWork() {}

// Timeout allows the full minute the validation permits.
func (Sleep) Timeout() time.Duration { return 90 * time.Second }

// Describe documents the operation.
func (Sleep) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Wait and answer (debug)", Tags: []string{"debug"}}
}

// RegisterDebug registers the debug requests on m. NewMediator calls it
// when Deps.Debug is set.
func RegisterDebug(m *mediator.Mediator, nodeID string) error {
	return mediator.HandleFunc(m, func(ctx context.Context, q Sleep) (SleepResult, error) {
		select {
		case <-ctx.Done():
			return SleepResult{}, mediator.Wrap(mediator.CodeTimeout, "sleep interrupted", ctx.Err())
		case <-time.After(time.Duration(q.Millis) * time.Millisecond):
			return SleepResult{SleptMillis: q.Millis, Node: nodeID}, nil
		}
	})
}

// NewMediator builds the service mediator: the standard behavior set of
// spec section 5 over deps (RequireAuthByDefault on, so every request must
// declare what it requires), every registration of RegisterWith, the
// httpapi route check at Build, and Build itself.
func NewMediator(deps Deps) (*mediator.Mediator, error) {
	var opts []mediator.Option
	if deps.Logger != nil {
		opts = append(opts, mediator.WithLogger(deps.Logger))
	}
	if deps.NodeID != "" {
		opts = append(opts, mediator.WithNodeID(deps.NodeID))
	}
	if deps.Remote != nil {
		opts = append(opts, mediator.WithRemote(deps.Remote))
	}
	m := mediator.New(opts...)
	cfg := behavior.Config{
		Logger:               deps.Logger,
		Tracer:               deps.Tracer,
		Meter:                deps.Meter,
		Store:                deps.Store,
		Cache:                deps.Cache,
		Limiter:              deps.Limiter,
		RequireAuthByDefault: true,
	}
	if err := behavior.UseStandard(m, cfg); err != nil {
		return nil, err
	}
	if err := RegisterWith(m, deps); err != nil {
		return nil, err
	}
	if deps.Debug {
		if err := RegisterDebug(m, deps.NodeID); err != nil {
			return nil, err
		}
	}
	if err := m.OnBuild(httpapi.BuildCheck); err != nil {
		return nil, err
	}
	if err := m.Build(); err != nil {
		return nil, err
	}
	return m, nil
}

// Registry builds the service mediator with no infrastructure at all: no
// store, cache, limiter, or querier. mediatorctl uses it for `names` and
// `openapi export`, which only walk the registry.
func Registry() (*mediator.Mediator, error) { return NewMediator(Deps{}) }

// OpenAPIConfig is the generator configuration of the service's document.
func OpenAPIConfig() openapi.Config {
	return openapi.Config{
		Info: openapi.Info{
			Title:       "Orders",
			Version:     Version,
			Description: "Example order service of the mediator framework (spec section 13). Every operation is one registered request; the document is generated from the registry.",
		},
		Prefix: "",
	}
}
