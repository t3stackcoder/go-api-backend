package workload

import (
	"context"
	"sort"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Persisted names (spec 4.11). Every request and event pins its name with
// Named so that renaming a Go type never changes the outbox, inbox,
// idempotency scope, or stream keys of a recorded run.
const (
	NameSetValue       = "wl.SetValue"
	NameGetValue       = "wl.GetValue"
	NameGetValueCached = "wl.GetValueCached"
	NameTransfer       = "wl.Transfer"
	NameReadAll        = "wl.ReadAll"
	NameAppend         = "wl.Append"
	NameReadList       = "wl.ReadList"
	NameBump           = "wl.Bump"
	NameAtomicScenario = "wl.AtomicScenario"
	NamePanic          = "wl.Panic"
	NameSlow           = "wl.Slow"
	NameTouch          = "wl.Touch"
	NameBumped         = "wl.Bumped"
	NameAtomicDone     = "wl.AtomicDone"

	// TopicBumped is the stream topic of Bumped.
	TopicBumped = "wl.bumped"

	// Consumer groups.
	GroupReadModel = "read_model"
	GroupAudit     = "audit"
	GroupPoison    = "poison"

	// HeaderCmd is the envelope header carrying the ID of the command that
	// published a durable event. I1 joins outbox rows to commands on it.
	HeaderCmd = "cmd"

	// CacheTTL is the TTL of GetValueCached.
	CacheTTL = 30 * time.Second
)

// AllGroups lists every consumer group the workload can register.
var AllGroups = []string{GroupReadModel, GroupAudit, GroupPoison}

// DefaultGroups are the groups of the events workload (spec 11.6): the
// projection and the audit log. The poison group is opt-in for DLQ tests.
var DefaultGroups = []string{GroupReadModel, GroupAudit}

// RegisterTag is the cache tag of one register key.
func RegisterTag(key string) string { return "wl:register:" + key }

// Names lists every persisted identifier of the workload, sorted: request
// and event names, the topic, and the consumer groups.
func Names() []string {
	out := []string{
		NameSetValue, NameGetValue, NameGetValueCached, NameTransfer, NameReadAll, NameAppend, NameReadList,
		NameBump, NameAtomicScenario, NamePanic, NameSlow, NameTouch, NameBumped, NameAtomicDone, TopicBumped,
	}
	out = append(out, AllGroups...)
	sort.Strings(out)
	return out
}

// SetValue writes a register key (register workloads). It is keyed on CmdID
// when Keyed is set, and bumps the cache tag of the key when Invalidate is
// set. It writes wl_register and wl_cmd_log.
type SetValue struct {
	mediator.Command[mediator.Void]

	Key        string `json:"key"        validate:"required,max=64"`
	Val        int64  `json:"val"`
	CmdID      string `json:"cmdId"      validate:"required,max=200"`
	Keyed      bool   `json:"keyed"`
	Invalidate bool   `json:"invalidate"`
}

func (SetValue) Name() string { return NameSetValue }

// IdempotencyKey is CmdID when the command is keyed, else "".
func (c SetValue) IdempotencyKey() string {
	if c.Keyed {
		return c.CmdID
	}
	return ""
}

// Invalidates bumps the register tag of Key when Invalidate is set.
func (c SetValue) Invalidates() []string {
	if c.Invalidate {
		return []string{RegisterTag(c.Key)}
	}
	return nil
}

// ValueView is the result of GetValue and GetValueCached.
type ValueView struct {
	Key   string `json:"key"`
	Val   int64  `json:"val"`
	Found bool   `json:"found"`
}

// GetValue reads a register key without caching.
type GetValue struct {
	mediator.Query[ValueView]

	Key string `json:"key" validate:"required,max=64"`
}

func (GetValue) Name() string { return NameGetValue }

// GetValueCached reads a register key through the cache (register-cached
// and cache-staleness workloads).
type GetValueCached struct {
	mediator.Query[ValueView]

	Key string `json:"key" validate:"required,max=64"`
}

func (GetValueCached) Name() string { return NameGetValueCached }

// CacheTags names the tag of the key.
func (q GetValueCached) CacheTags() []string { return []string{RegisterTag(q.Key)} }

// CacheTTL is 30 s.
func (GetValueCached) CacheTTL() time.Duration { return CacheTTL }

// Transfer moves Amt from one account to another in one transaction (bank
// workloads). It fails with CodePrecondition when the source balance is
// insufficient and with CodeNotFound for an unknown account. Keyed on CmdID
// when Keyed is set.
type Transfer struct {
	mediator.Command[mediator.Void]

	From  string `json:"from"  validate:"required,max=64"`
	To    string `json:"to"    validate:"required,max=64"`
	Amt   int64  `json:"amt"   validate:"gt=0"`
	CmdID string `json:"cmdId" validate:"required,max=200"`
	Keyed bool   `json:"keyed"`
}

func (Transfer) Name() string { return NameTransfer }

// IdempotencyKey is CmdID when the command is keyed, else "".
func (c Transfer) IdempotencyKey() string {
	if c.Keyed {
		return c.CmdID
	}
	return ""
}

// Validate rejects a transfer to the same account.
func (c Transfer) Validate(context.Context) error {
	if c.From == c.To {
		return (&mediator.ValidationError{}).Add("/to", "different", "to must differ from from")
	}
	return nil
}

// BankView is the result of ReadAll.
type BankView struct {
	Balances map[string]int64 `json:"balances"`
	Total    int64            `json:"total"`
}

// ReadAll reads every balance in one snapshot.
type ReadAll struct {
	mediator.Query[BankView]
}

func (ReadAll) Name() string { return NameReadAll }

// AppendResult is the result of Append: the apply serial of the row.
type AppendResult struct {
	Applied int64 `json:"applied"`
}

// Append adds Val to the list of Key (idempotent-append workload). It is
// always keyed on CmdID, writes one wl_appends row, and increments
// wl_executions.
type Append struct {
	mediator.Command[AppendResult]

	Key   string `json:"key"   validate:"required,max=64"`
	Val   int64  `json:"val"`
	CmdID string `json:"cmdId" validate:"required,max=200"`
}

func (Append) Name() string { return NameAppend }

// IdempotencyKey is CmdID.
func (c Append) IdempotencyKey() string { return c.CmdID }

// ReadList reads the values appended to Key in apply order.
type ReadList struct {
	mediator.Query[[]int64]

	Key string `json:"key" validate:"required,max=64"`
}

func (ReadList) Name() string { return NameReadList }

// BumpResult is the result of Bump: the new counter value.
type BumpResult struct {
	N int64 `json:"n"`
}

// Bump increments the counter of Key and publishes Bumped{Key, N} (events
// workload). Keyed on CmdID when CmdID is set.
type Bump struct {
	mediator.Command[BumpResult]

	Key   string `json:"key"             validate:"required,max=64"`
	CmdID string `json:"cmdId,omitzero" validate:"max=200"`
}

func (Bump) Name() string { return NameBump }

// IdempotencyKey is CmdID, "" when unset.
func (c Bump) IdempotencyKey() string { return c.CmdID }

// Bumped is the durable event of Bump: one per increment, ordered per key.
type Bumped struct {
	mediator.Event

	Key string `json:"key"`
	N   int64  `json:"n"`
}

func (Bumped) Name() string { return NameBumped }

// StreamKey is the bumped key.
func (e Bumped) StreamKey() string { return e.Key }

// Topic is TopicBumped.
func (Bumped) Topic() string { return TopicBumped }

// AtomicScenario is the command of SweepCommandAtomicity (spec 11.4): it
// writes wl_register for Key1, bumps Key1 and Key2 (two durable Bumped on
// two keys), publishes AtomicDone whose in-process handler writes wl_side,
// and is keyed on CmdKey when CmdKey is set. Either every row commits or
// none does (G4, I1).
type AtomicScenario struct {
	mediator.Command[mediator.Void]

	CmdID  string `json:"cmdId"           validate:"required,max=200"`
	Key1   string `json:"key1"            validate:"required,max=64"`
	Key2   string `json:"key2"            validate:"required,max=64"`
	Val    int64  `json:"val"`
	CmdKey string `json:"cmdKey,omitzero" validate:"max=200"`
}

func (AtomicScenario) Name() string { return NameAtomicScenario }

// IdempotencyKey is CmdKey, "" when unset.
func (c AtomicScenario) IdempotencyKey() string { return c.CmdKey }

// AtomicDone is the in-process notification of AtomicScenario; its handler
// writes wl_side in the publisher's transaction.
type AtomicDone struct {
	mediator.Event

	CmdID string `json:"cmdId"`
	Note  string `json:"note"`
}

func (AtomicDone) Name() string { return NameAtomicDone }

// Panic is a command whose handler panics (G3).
type Panic struct {
	mediator.Command[mediator.Void]
}

func (Panic) Name() string { return NamePanic }

// Slow is a command whose handler sleeps for Millis, honoring the context
// (timeout and shutdown tests).
type Slow struct {
	mediator.Command[mediator.Void]

	Millis int `json:"millis" validate:"gte=0,lte=600000"`
}

func (Slow) Name() string { return NameSlow }

// Touch is the command the projector sends through the mediator from inside
// a consumer handler (Deps.NestedTouch) to exercise nested Send in a
// consumer unit of work. It writes only wl_cmd_log.
type Touch struct {
	mediator.Command[mediator.Void]

	Key   string `json:"key"   validate:"required,max=64"`
	CmdID string `json:"cmdId" validate:"required,max=200"`
}

func (Touch) Name() string { return NameTouch }
