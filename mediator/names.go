package mediator

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// NamePattern is the grammar every persisted name must match.
var NamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.]{0,127}$`)

// Behavior name constants of the standard set. behavior re-exports them.
const (
	NameRecovery      = "Recovery"
	NameTracing       = "Tracing"
	NameLogging       = "Logging"
	NameMetrics       = "Metrics"
	NameTimeout       = "Timeout"
	NameAuthorization = "Authorization"
	NameRateLimit     = "RateLimit"
	NameValidation    = "Validation"
	NameCache         = "Cache"
	NameRetry         = "Retry"
	NameUnitOfWork    = "UnitOfWork"
	NameIdempotency   = "Idempotency"
	NameInbox         = "Inbox"
)

// deriveName returns the persisted name of a request or event type: Name()
// when the type implements Named, else the Go type name.
func deriveName(t reflect.Type) (string, error) {
	var name string
	if t.Implements(namedT) {
		name = reflect.Zero(t).Interface().(Named).Name()
	} else {
		name = t.Name()
		if i := strings.IndexByte(name, '['); i >= 0 { // generic instantiation
			name = name[:i]
		}
	}
	if !NamePattern.MatchString(name) {
		return name, fmt.Errorf("mediator: name %q of %s does not match %s", name, t, NamePattern)
	}
	return name, nil
}

// NameEntry is one persisted identifier the registry derived.
type NameEntry struct {
	Kind   Kind
	Name   string
	GoType string
	Group  string // consumers only
	Topic  string // durable events only
	Pinned bool   // true when the type implements Named
}

// Names lists every persisted name the registry derives, sorted by kind then
// name. mediatorctl names prints it.
func (m *Mediator) Names() []NameEntry {
	var out []NameEntry
	for _, r := range m.requests {
		out = append(out, NameEntry{Kind: r.info.Kind, Name: r.info.Name, GoType: r.info.RequestType.String(), Pinned: r.info.Traits.Named})
	}
	for _, n := range m.notifications {
		out = append(out, NameEntry{Kind: KindNotification, Name: n.info.Name, GoType: n.info.RequestType.String(), Topic: n.info.Topic, Pinned: n.info.Traits.Named})
	}
	for _, c := range m.consumers {
		out = append(out, NameEntry{Kind: KindConsumer, Name: c.info.Name, GoType: c.info.RequestType.String(), Group: c.group, Topic: c.info.Topic, Pinned: c.info.Traits.Named})
	}
	sortNames(out)
	return out
}

func sortNames(out []NameEntry) {
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && less(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
}

func less(a, b NameEntry) bool {
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	return a.Group < b.Group
}
