package memstore

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// StreamEntry is one entry of an in-memory partition stream.
type StreamEntry struct {
	ID    string
	Entry pg.OutboxEntry
}

// StreamHooks inject failures into Streams. The hooks are read without
// locking: install them before the Streams is shared with another goroutine
// (a running relay slot) and switch a failure on and off inside the hook, with
// an atomic, rather than by reassigning the hook while that goroutine runs.
type StreamHooks struct {
	// Append runs before an append; an error fails it without adding anything.
	Append func(topic string, partition int, entries []pg.OutboxEntry) error
	// Tail runs before Tail; an error fails it.
	Tail func(topic string, partition int) error
}

type streamKey struct {
	topic     string
	partition int
}

type stream struct {
	entries []StreamEntry
	groups  map[string]bool
	lastMs  int64
	lastSeq int64
}

// Streams is an in-memory pg.StreamSink and pg.StreamTrimmer that records
// appended entries per (topic, partition) with Redis-style "<ms>-<seq>" IDs.
type Streams struct {
	Hooks   StreamHooks
	mu      sync.Mutex
	clock   testkit.Clock
	streams map[streamKey]*stream
	appends int
}

// NewStreams returns an empty sink. A nil clock means RealClock.
func NewStreams(clock testkit.Clock) *Streams {
	if clock == nil {
		clock = testkit.RealClock{}
	}
	return &Streams{clock: clock, streams: map[streamKey]*stream{}}
}

func (s *Streams) get(topic string, partition int, create bool) *stream {
	k := streamKey{topic, partition}
	st := s.streams[k]
	if st == nil && create {
		st = &stream{groups: map[string]bool{}}
		s.streams[k] = st
	}
	return st
}

func (st *stream) nextID(now time.Time) string {
	ms := now.UnixMilli()
	if ms <= st.lastMs {
		st.lastSeq++
	} else {
		st.lastMs, st.lastSeq = ms, 0
	}
	return fmt.Sprintf("%d-%d", st.lastMs, st.lastSeq)
}

// Append adds the entries in order and returns the ID of the last one.
func (s *Streams) Append(ctx context.Context, topic string, partition int, entries []pg.OutboxEntry) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if h := s.Hooks.Append; h != nil {
		if err := h(topic, partition, entries); err != nil {
			return "", err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(topic, partition, true)
	var last string
	for _, e := range entries {
		last = st.nextID(s.clock.Now())
		st.entries = append(st.entries, StreamEntry{ID: last, Entry: e})
	}
	s.appends++
	return last, nil
}

// Tail returns the stream ID and outbox ID of the last entry; ok is false
// when the stream is missing or empty.
func (s *Streams) Tail(ctx context.Context, topic string, partition int) (string, int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, false, err
	}
	if h := s.Hooks.Tail; h != nil {
		if err := h(topic, partition); err != nil {
			return "", 0, false, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(topic, partition, false)
	if st == nil || len(st.entries) == 0 {
		return "", 0, false, nil
	}
	last := st.entries[len(st.entries)-1]
	return last.ID, last.Entry.ID, true, nil
}

// EnsureGroups records the consumer groups, creating the stream if needed.
func (s *Streams) EnsureGroups(ctx context.Context, topic string, partition int, groups []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(topic, partition, true)
	for _, g := range groups {
		st.groups[g] = true
	}
	return nil
}

// TrimBefore removes entries whose ID timestamp is before minTime.
func (s *Streams) TrimBefore(ctx context.Context, topic string, partition int, minTime time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(topic, partition, false)
	if st == nil {
		return nil
	}
	cut := minTime.UnixMilli()
	kept := st.entries[:0]
	for _, e := range st.entries {
		ms, _ := strconv.ParseInt(e.ID[:strings.IndexByte(e.ID, '-')], 10, 64)
		if ms >= cut {
			kept = append(kept, e)
		}
	}
	st.entries = kept
	return nil
}

// Entries returns a copy of the stream's entries in order.
func (s *Streams) Entries(topic string, partition int) []StreamEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(topic, partition, false)
	if st == nil {
		return nil
	}
	out := make([]StreamEntry, len(st.entries))
	copy(out, st.entries)
	return out
}

// Groups returns the stream's consumer groups, sorted.
func (s *Streams) Groups(topic string, partition int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.get(topic, partition, false)
	if st == nil {
		return nil
	}
	out := make([]string, 0, len(st.groups))
	for g := range st.groups {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// Drop deletes the stream and its groups, simulating Redis data loss.
func (s *Streams) Drop(topic string, partition int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, streamKey{topic, partition})
}

// Truncate keeps only the first keep entries, simulating a restore from an
// old snapshot.
func (s *Streams) Truncate(topic string, partition int, keep int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.get(topic, partition, false); st != nil && keep < len(st.entries) {
		st.entries = st.entries[:keep]
	}
}

// Appends counts Append calls that succeeded.
func (s *Streams) Appends() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appends
}
