package httpapi

import (
	"context"
	"log/slog"
	"sync/atomic"
)

type remoteAddrKey struct{}

// WithRemoteAddr returns a context carrying the client IP address of the
// request. The adapter sets it before dispatch; the rate limiter uses it as
// the default key when the request has no principal.
func WithRemoteAddr(ctx context.Context, addr string) context.Context {
	return context.WithValue(ctx, remoteAddrKey{}, addr)
}

// RemoteAddr returns the client IP address recorded by WithRemoteAddr, or ""
// outside an HTTP request. It is the peer address of the connection; proxy
// headers such as X-Forwarded-For are not consulted.
func RemoteAddr(ctx context.Context) string {
	s, _ := ctx.Value(remoteAddrKey{}).(string)
	return s
}

// CacheResult is what the cache behavior reports about one query. The
// adapter echoes it in the X-Mediator-Cache response header.
type CacheResult string

const (
	// CacheHit means the response was served from the cache.
	CacheHit CacheResult = "hit"
	// CacheMiss means the handler ran and the response was stored.
	CacheMiss CacheResult = "miss"
	// CacheBypass means the cache was skipped (Cache-Control: no-cache or a
	// degraded cache).
	CacheBypass CacheResult = "bypass"
)

type cacheResultKey struct{}

// cacheResultHolder is the mutable cell stored in the context before Send so
// a behavior deeper in the pipeline can report to the adapter after the fact.
type cacheResultHolder struct{ v atomic.Pointer[CacheResult] }

// WithCacheResultHolder returns a context in which SetCacheResult records a
// value that CacheResultFrom later returns. The adapter installs one before
// every Send; other adapters and tests of the cache behavior can do the same.
func WithCacheResultHolder(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheResultKey{}, &cacheResultHolder{})
}

// SetCacheResult records the cache outcome of the current request so the
// adapter can set X-Mediator-Cache. It is a no-op when the context was not
// prepared with WithCacheResultHolder. The last value set wins.
func SetCacheResult(ctx context.Context, r CacheResult) {
	if h, ok := ctx.Value(cacheResultKey{}).(*cacheResultHolder); ok {
		h.v.Store(&r)
	}
}

// CacheResultFrom returns the cache outcome recorded with SetCacheResult, and
// false when nothing was recorded or the context carries no holder.
func CacheResultFrom(ctx context.Context) (CacheResult, bool) {
	h, ok := ctx.Value(cacheResultKey{}).(*cacheResultHolder)
	if !ok {
		return "", false
	}
	p := h.v.Load()
	if p == nil {
		return "", false
	}
	return *p, true
}

type loggerKey struct{}

// withLogger attaches the server logger so WriteProblem, a package-level
// function, can log internal errors through the configured logger.
func withLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

func loggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

type drainKey struct{}

// withDrainSignal attaches the channel the Listener closes when shutdown
// begins. SSE streams end when it closes; ordinary requests are allowed to
// finish.
func withDrainSignal(ctx context.Context, ch <-chan struct{}) context.Context {
	return context.WithValue(ctx, drainKey{}, ch)
}

// drainSignal returns the shutdown channel, or nil (never ready) outside a
// Listener.
func drainSignal(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(drainKey{}).(<-chan struct{})
	return ch
}
