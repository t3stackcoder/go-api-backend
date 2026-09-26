package behavior

import (
	"encoding"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Redacted replaces the value of every field tagged log:"-" or log:"redact".
const Redacted = "[redacted]"

// maxRedactDepth bounds recursion through self-referential values.
const maxRedactDepth = 32

// redactor converts a request into a loggable map[string]any with the
// redaction rules of 5.4 applied at every depth: structs become maps keyed
// by JSON name, slices and arrays become []any, maps become map[string]any,
// pointers and interfaces are dereferenced, and a field tagged log:"-" or
// log:"redact" becomes Redacted whatever its value. Struct plans are
// compiled once per type.
type redactor struct {
	plans sync.Map // reflect.Type -> *redactPlan
}

type redactPlan struct {
	fields []redactField
}

type redactField struct {
	name    string
	index   int
	redact  bool
	flatten bool // embedded struct without a JSON name: merge its fields
}

// Redact returns the loggable form of v.
func (r *redactor) Redact(v any) any {
	return r.value(reflect.ValueOf(v), 0)
}

var (
	textMarshalerT = reflect.TypeFor[encoding.TextMarshaler]()
	jsonMarshalerT = reflect.TypeFor[interface{ MarshalJSON() ([]byte, error) }]()
	timeT          = reflect.TypeFor[time.Time]()
)

// opaque reports whether a struct is logged as one value rather than
// expanded: time.Time and types that marshal themselves.
func opaque(t reflect.Type) bool {
	if t == timeT {
		return true
	}
	return t.Implements(textMarshalerT) || reflect.PointerTo(t).Implements(textMarshalerT) ||
		t.Implements(jsonMarshalerT) || reflect.PointerTo(t).Implements(jsonMarshalerT)
}

func (r *redactor) value(v reflect.Value, depth int) any {
	if depth > maxRedactDepth {
		return "[depth exceeded]"
	}
	switch v.Kind() {
	case reflect.Invalid:
		return nil
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return r.value(v.Elem(), depth+1)
	case reflect.Struct:
		if opaque(v.Type()) && v.CanInterface() {
			return v.Interface()
		}
		// A self-marshaling struct reached through an unexported embedded
		// field cannot be handed out as an interface; expand it instead.
		out := make(map[string]any)
		r.fill(out, v, depth)
		return out
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Bytes()
		}
		return r.list(v, depth)
	case reflect.Array:
		return r.list(v, depth)
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out[mapKey(iter.Key())] = r.value(iter.Value(), depth+1)
		}
		return out
	default:
		return scalar(v)
	}
}

// scalar extracts a leaf value. Values reached through an unexported
// embedded struct carry the read-only flag and cannot be handed out through
// Interface, so their kind-specific accessors are used instead.
func scalar(v reflect.Value) any {
	if v.CanInterface() {
		return v.Interface()
	}
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Complex64, reflect.Complex128:
		return v.Complex()
	case reflect.String:
		return v.String()
	default:
		return nil // chan, func, unsafe pointer
	}
}

func (r *redactor) list(v reflect.Value, depth int) []any {
	out := make([]any, v.Len())
	for i := range out {
		out[i] = r.value(v.Index(i), depth+1)
	}
	return out
}

func mapKey(k reflect.Value) string {
	if k.Kind() == reflect.String {
		return k.String()
	}
	if k.CanInterface() {
		return fmt.Sprint(k.Interface())
	}
	return k.String()
}

func (r *redactor) fill(out map[string]any, v reflect.Value, depth int) {
	for _, f := range r.plan(v.Type()).fields {
		fv := v.Field(f.index)
		switch {
		case f.redact:
			out[f.name] = Redacted
		case f.flatten:
			for fv.Kind() == reflect.Pointer {
				if fv.IsNil() {
					break
				}
				fv = fv.Elem()
			}
			if fv.Kind() == reflect.Struct && depth < maxRedactDepth {
				r.fill(out, fv, depth+1)
			}
		default:
			out[f.name] = r.value(fv, depth+1)
		}
	}
}

func (r *redactor) plan(t reflect.Type) *redactPlan {
	if p, ok := r.plans.Load(t); ok {
		return p.(*redactPlan)
	}
	p := &redactPlan{}
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		ft := sf.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if mediator.IsMarker(ft) || (!sf.IsExported() && !sf.Anonymous) {
			continue
		}
		name, explicit := jsonName(sf)
		if name == "-" {
			continue
		}
		logTag := sf.Tag.Get("log")
		f := redactField{name: name, index: i, redact: logTag == "-" || logTag == "redact"}
		if sf.Anonymous && !explicit && !f.redact && ft.Kind() == reflect.Struct && !opaque(ft) {
			f.flatten = true
		}
		if !sf.IsExported() && !f.flatten {
			continue
		}
		p.fields = append(p.fields, f)
	}
	actual, _ := r.plans.LoadOrStore(t, p)
	return actual.(*redactPlan)
}

// jsonName returns the JSON member name of a field and whether the json tag
// set it explicitly.
func jsonName(sf reflect.StructField) (string, bool) {
	tag, ok := sf.Tag.Lookup("json")
	if !ok {
		return sf.Name, false
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return sf.Name, false
	}
	return name, true
}
