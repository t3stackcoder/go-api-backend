// Package cachemodel is the executable model of the cache protocol of spec
// 7.4: an in-memory backend with the exact semantics of the Lua scripts,
// and a scheduler that interleaves the steps of concurrent readers and
// writers and checks the freshness bound of G9. PropCacheProtocol drives
// the scheduler with rapid; test/faultsweep reuses it against a real
// redisx.Cache, which satisfies Backend.
package cachemodel

import (
	"context"
	"maps"
	"sync"
	"time"
)

// Backend is the cache protocol surface (identical to behavior.CacheBackend).
type Backend interface {
	Get(ctx context.Context, key string) (body []byte, ok bool, err error)
	SnapshotTags(ctx context.Context, tags []string) (map[string]int64, error)
	Set(ctx context.Context, key string, snapshot map[string]int64, body []byte, ttl time.Duration) error
	BumpTagsPre(ctx context.Context, tags []string) error
	BumpTagsPost(ctx context.Context, tags []string) error
}

// Operation names passed to Memory.Fail.
const (
	OpGet      = "get"
	OpSnapshot = "snapshot"
	OpSet      = "set"
	OpBumpPre  = "bump.pre"
	OpBumpPost = "bump.post"
)

// Memory is an in-memory Backend with the semantics of 7.4: cache_get
// verifies every recorded tag version and unlinks a stale entry; cache_set
// stores only when no tag moved since the snapshot; bumps are INCRs.
// Entries expire by TTL against Now. Fail scripts failures; every
// operation first checks the context.
type Memory struct {
	// Now is the clock for TTL expiry. Default time.Now.
	Now func() time.Time
	// Fail, when set, is consulted with the operation name before it runs;
	// a non-nil error is returned instead of performing it.
	Fail func(op string) error

	mu      sync.Mutex
	tags    map[string]int64
	entries map[string]entry
	ops     map[string]int
}

type entry struct {
	versions map[string]int64
	body     []byte
	expires  time.Time
	forever  bool
}

// NewMemory returns an empty backend.
func NewMemory() *Memory {
	return &Memory{Now: time.Now, tags: map[string]int64{}, entries: map[string]entry{}, ops: map[string]int{}}
}

func (m *Memory) begin(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	m.ops[op]++
	m.mu.Unlock()
	if m.Fail != nil {
		return m.Fail(op)
	}
	return nil
}

// Get runs cache_get.
func (m *Memory) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := m.begin(ctx, OpGet); err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return nil, false, nil
	}
	if !e.forever && !m.Now().Before(e.expires) {
		delete(m.entries, key)
		return nil, false, nil
	}
	for tag, ver := range e.versions {
		if m.tags[tag] != ver {
			delete(m.entries, key)
			return nil, false, nil
		}
	}
	return append([]byte(nil), e.body...), true, nil
}

// SnapshotTags returns the current version of each tag; missing tags are 0.
func (m *Memory) SnapshotTags(ctx context.Context, tags []string) (map[string]int64, error) {
	if err := m.begin(ctx, OpSnapshot); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(tags))
	for _, t := range tags {
		out[t] = m.tags[t]
	}
	return out, nil
}

// Set runs cache_set: the entry is stored only when every tag still has
// its snapshot version. A ttl of zero or less stores without expiry.
func (m *Memory) Set(ctx context.Context, key string, snapshot map[string]int64, body []byte, ttl time.Duration) error {
	if err := m.begin(ctx, OpSet); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for tag, ver := range snapshot {
		if m.tags[tag] != ver {
			return nil
		}
	}
	e := entry{versions: maps.Clone(snapshot), body: append([]byte(nil), body...)}
	if ttl > 0 {
		e.expires = m.Now().Add(ttl)
	} else {
		e.forever = true
	}
	m.entries[key] = e
	return nil
}

// BumpTagsPre increments every tag.
func (m *Memory) BumpTagsPre(ctx context.Context, tags []string) error {
	return m.bump(ctx, OpBumpPre, tags)
}

// BumpTagsPost increments every tag.
func (m *Memory) BumpTagsPost(ctx context.Context, tags []string) error {
	return m.bump(ctx, OpBumpPost, tags)
}

func (m *Memory) bump(ctx context.Context, op string, tags []string) error {
	if err := m.begin(ctx, op); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range tags {
		m.tags[t]++
	}
	return nil
}

// Version returns the current version of a tag.
func (m *Memory) Version(tag string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tags[tag]
}

// Entry returns the stored body of a key without the version check.
func (m *Memory) Entry(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), e.body...), true
}

// Ops returns how many times an operation was attempted.
func (m *Memory) Ops(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ops[op]
}

// Reset clears entries, tags, and counters.
func (m *Memory) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tags = map[string]int64{}
	m.entries = map[string]entry{}
	m.ops = map[string]int{}
}
