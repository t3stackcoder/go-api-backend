package cachemodel

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config sizes one scheduled run.
type Config struct {
	// Readers and Writers are the number of concurrent actors.
	Readers, Writers int
	// Key is the cache key every reader uses; Tags are the tags the query
	// depends on and every writer invalidates.
	Key  string
	Tags []string
	// TTL is passed to Set. Zero stores without expiry.
	TTL time.Duration
	// SkipPreBump and SkipPostBump remove one bump of the protocol. They
	// exist so a test can show the model detects a broken protocol.
	SkipPreBump, SkipPostBump bool
}

// Reader and writer step names, in order of execution per actor.
const (
	StepGet      = "get"      // reader: cache_get; on a hit the read returns
	StepSnapshot = "snapshot" // reader: MGET tag versions
	StepReadDB   = "readdb"   // reader: read the committed value
	StepSet      = "set"      // reader: cache_set; the read returns the db value
	StepBegin    = "begin"    // writer: transaction opened
	StepBumpPre  = "bumppre"  // writer: pre-commit bump
	StepCommit   = "commit"   // writer: COMMIT; the value becomes visible
	StepBumpPost = "bumppost" // writer: post-commit bump
	StepReturn   = "return"   // writer: Send returns
)

// Step is one executed step of the history.
type Step struct {
	Actor int
	Op    string
	// Note carries the observed value or error of the step.
	Note string
}

func (s Step) String() string {
	if s.Note == "" {
		return fmt.Sprintf("%d:%s", s.Actor, s.Op)
	}
	return fmt.Sprintf("%d:%s(%s)", s.Actor, s.Op, s.Note)
}

// Violation is a read served from the cache that returned a value older
// than a write whose Send had returned before the read was invoked.
type Violation struct {
	Reader  int
	Got     int64
	Bound   int64
	History []Step
}

func (v *Violation) Error() string {
	steps := make([]string, len(v.History))
	for i, s := range v.History {
		steps[i] = s.String()
	}
	return fmt.Sprintf("cachemodel: reader %d served %d from cache after write %d returned; history: %s",
		v.Reader, v.Got, v.Bound, strings.Join(steps, " "))
}

type actor struct {
	id     int
	writer bool
	pc     int
	// reader state
	bound    int64
	snapshot map[string]int64
	value    int64
	// writer state
	written int64
}

var (
	readerSteps = []string{StepGet, StepSnapshot, StepReadDB, StepSet}
	writerSteps = []string{StepBegin, StepBumpPre, StepCommit, StepBumpPost, StepReturn}
)

func (a *actor) steps() []string {
	if a.writer {
		return writerSteps
	}
	return readerSteps
}

func (a *actor) done() bool { return a.pc >= len(a.steps()) }

// Scheduler runs readers and writers step by step in an order the caller
// chooses. The database is modeled as one committed version counter: a
// commit increments it and the writer's value is the new version, so a
// smaller value is an older state. The invariant checked on every cache
// hit is that of G9 and I7: the value is not older than the last write
// whose Send returned before the read was invoked.
type Scheduler struct {
	cfg     Config
	ctx     context.Context
	backend Backend

	committed    int64
	lastReturned int64
	actors       []*actor
	history      []Step
	degraded     []error
	violation    *Violation
}

// New prepares a run. It does not touch the backend.
func New(ctx context.Context, backend Backend, cfg Config) *Scheduler {
	s := &Scheduler{cfg: cfg, ctx: ctx, backend: backend}
	for i := 0; i < cfg.Readers; i++ {
		s.actors = append(s.actors, &actor{id: len(s.actors)})
	}
	for i := 0; i < cfg.Writers; i++ {
		s.actors = append(s.actors, &actor{id: len(s.actors), writer: true})
	}
	return s
}

// Runnable returns the actors that still have steps, in id order.
func (s *Scheduler) Runnable() []int {
	var out []int
	for _, a := range s.actors {
		if !a.done() {
			out = append(out, a.id)
		}
	}
	return out
}

// History returns the executed steps.
func (s *Scheduler) History() []Step { return append([]Step(nil), s.history...) }

// Degraded returns the backend errors observed. While non-empty the
// freshness bound is the TTL and hits are not checked.
func (s *Scheduler) Degraded() []error { return append([]error(nil), s.degraded...) }

// Violation returns the first violation, or nil.
func (s *Scheduler) Violation() *Violation { return s.violation }

// Run executes steps until no actor is runnable, choosing the next actor
// with pick. It returns the first violation.
func (s *Scheduler) Run(pick func(runnable []int) int) error {
	for {
		r := s.Runnable()
		if len(r) == 0 {
			break
		}
		if err := s.Step(pick(r)); err != nil {
			return err
		}
	}
	return nil
}

// Step executes the next step of one actor and returns a violation it
// caused, if any. Stepping a finished or unknown actor is a no-op.
func (s *Scheduler) Step(id int) error {
	if id < 0 || id >= len(s.actors) || s.actors[id].done() {
		return nil
	}
	a := s.actors[id]
	op := a.steps()[a.pc]
	a.pc++
	var err error
	if a.writer {
		err = s.writerStep(a, op)
	} else {
		err = s.readerStep(a, op)
	}
	return err
}

func (s *Scheduler) record(a *actor, op, note string) {
	s.history = append(s.history, Step{Actor: a.id, Op: op, Note: note})
}

func (s *Scheduler) fail(a *actor, op string, err error) {
	s.degraded = append(s.degraded, fmt.Errorf("%d:%s: %w", a.id, op, err))
	s.record(a, op, "error: "+err.Error())
}

func (s *Scheduler) readerStep(a *actor, op string) error {
	switch op {
	case StepGet:
		a.bound = s.lastReturned
		body, ok, err := s.backend.Get(s.ctx, s.cfg.Key)
		if err != nil {
			s.fail(a, op, err)
			return nil
		}
		if !ok {
			s.record(a, op, "miss")
			return nil
		}
		got, perr := strconv.ParseInt(string(body), 10, 64)
		if perr != nil {
			s.fail(a, op, fmt.Errorf("undecodable body %q", body))
			return nil
		}
		s.record(a, op, "hit "+strconv.FormatInt(got, 10))
		a.pc = len(readerSteps) // served from cache; the read returns
		if len(s.degraded) == 0 && got < a.bound {
			v := &Violation{Reader: a.id, Got: got, Bound: a.bound, History: s.History()}
			if s.violation == nil {
				s.violation = v
			}
			return v
		}
	case StepSnapshot:
		snap, err := s.backend.SnapshotTags(s.ctx, s.cfg.Tags)
		if err != nil {
			s.fail(a, op, err)
			return nil
		}
		a.snapshot = snap
		s.record(a, op, fmt.Sprint(snap))
	case StepReadDB:
		a.value = s.committed
		s.record(a, op, strconv.FormatInt(a.value, 10))
	default: // StepSet
		if a.snapshot == nil {
			s.record(a, op, "skipped")
			return nil
		}
		if err := s.backend.Set(s.ctx, s.cfg.Key, a.snapshot, []byte(strconv.FormatInt(a.value, 10)), s.cfg.TTL); err != nil {
			s.fail(a, op, err)
			return nil
		}
		s.record(a, op, strconv.FormatInt(a.value, 10))
	}
	return nil
}

func (s *Scheduler) writerStep(a *actor, op string) error {
	switch op {
	case StepBegin:
		s.record(a, op, "")
	case StepBumpPre:
		if s.cfg.SkipPreBump {
			s.record(a, op, "skipped")
			return nil
		}
		if err := s.backend.BumpTagsPre(s.ctx, s.cfg.Tags); err != nil {
			s.fail(a, op, err)
			return nil
		}
		s.record(a, op, "")
	case StepCommit:
		s.committed++
		a.written = s.committed
		s.record(a, op, strconv.FormatInt(a.written, 10))
	case StepBumpPost:
		if s.cfg.SkipPostBump {
			s.record(a, op, "skipped")
			return nil
		}
		if err := s.backend.BumpTagsPost(s.ctx, s.cfg.Tags); err != nil {
			s.fail(a, op, err)
			return nil
		}
		s.record(a, op, "")
	default: // StepReturn
		if a.written > s.lastReturned {
			s.lastReturned = a.written
		}
		s.record(a, op, strconv.FormatInt(a.written, 10))
	}
	return nil
}
