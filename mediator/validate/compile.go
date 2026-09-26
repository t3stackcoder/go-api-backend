package validate

import (
	"context"
	"encoding"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// jsonMarshalerV1 is encoding/json's Marshaler, declared locally so the
// package does not import the v1 API.
type jsonMarshalerV1 interface{ MarshalJSON() ([]byte, error) }

var (
	timeType            = reflect.TypeFor[time.Time]()
	uuidType            = reflect.TypeFor[uuid.UUID]()
	rawType             = reflect.TypeFor[jsontext.Value]()
	byteType            = reflect.TypeFor[uint8]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	jsonMarshalerType   = reflect.TypeFor[jsonMarshalerV1]()
	jsonMarshalerToType = reflect.TypeFor[json.MarshalerTo]()
)

// implementsEither reports whether t or *t implements iface.
func implementsEither(t reflect.Type, iface reflect.Type) bool {
	return t.Implements(iface) || reflect.PointerTo(t).Implements(iface)
}

// isJSONMarshaler reports whether t encodes itself with MarshalJSON or
// MarshalJSONTo, in which case its JSON shape is unknown.
func isJSONMarshaler(t reflect.Type) bool {
	return implementsEither(t, jsonMarshalerType) || implementsEither(t, jsonMarshalerToType)
}

// isTextMarshaler reports whether t encodes as a JSON string via MarshalText.
func isTextMarshaler(t reflect.Type) bool { return implementsEither(t, textMarshalerType) }

// isOpaque reports whether t is checked for presence only: its JSON shape is
// fixed by the encoder, not by the Go kind.
func isOpaque(t reflect.Type) bool {
	switch t {
	case timeType, uuidType, rawType:
		return true
	}
	if k := t.Kind(); k == reflect.Interface || k == reflect.Bool {
		return true
	}
	return isJSONMarshaler(t) || (t.Kind() != reflect.String && isTextMarshaler(t))
}

// isBytes reports whether t encodes as a base64 string.
func isBytes(t reflect.Type) bool {
	return (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem() == byteType
}

// fieldPlan is one JSON-visible field of a struct plan.
type fieldPlan struct {
	name   string // JSON member name
	seg    string // name escaped as a JSON Pointer token
	index  []int  // path through embedded structs
	single int    // index[0] when the field is direct, else -1
	typ    reflect.Type
	sf     reflect.StructField
	doc    string
	c      checker // nil when nothing below needs checking
}

// value fetches the field from its struct; ok is false when an embedded
// pointer on the path is nil.
func (f *fieldPlan) value(v reflect.Value) (reflect.Value, bool) {
	if f.single >= 0 {
		return v.Field(f.single), true
	}
	return fieldByIndex(v, f.index)
}

// fieldByIndex walks index through embedded fields, stopping at a nil
// embedded pointer.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v, true
}

// structPlan is the compiled plan of one struct type.
type structPlan struct {
	typ         reflect.Type
	fields      []fieldPlan
	ownMode     validateMode // the struct's Validate, declared or promoted
	hasCheck    bool         // some field has a checker or hasValidate
	hasValidate bool         // a Validate method exists at or below this struct
	done        bool         // false while the plan is being built (cycles)
}

// check runs the tag rules of every field.
func (p *structPlan) check(cc *checkCtx, v reflect.Value) {
	for i := range p.fields {
		f := &p.fields[i]
		if f.c == nil {
			continue
		}
		fv, ok := f.value(v)
		cc.push(f.seg)
		if ok {
			f.c.check(cc, fv)
		} else if f.c.required() {
			cc.fail("required", "is required")
		}
		cc.pop()
	}
}

// validate runs Validate methods depth first: nested structs first, then the
// struct itself. An embedded struct's Validate takes part through Go method
// promotion (it is the outer struct's Validate unless the outer declares its
// own), so it is never called separately.
func (p *structPlan) validate(ctx context.Context, cc *checkCtx, v reflect.Value) error {
	for i := range p.fields {
		f := &p.fields[i]
		if f.c == nil || !f.c.needsValidate() {
			continue
		}
		fv, ok := f.value(v)
		if !ok {
			continue
		}
		cc.push(f.seg)
		err := f.c.validate(ctx, cc, fv)
		cc.pop()
		if err != nil {
			return err
		}
	}
	if p.ownMode != validateNone {
		return cc.runValidate(ctx, v, p.ownMode)
	}
	return nil
}

// rawField is a JSON-visible field found while flattening a struct.
type rawField struct {
	sf      reflect.StructField
	index   []int
	name    string
	hasName bool
}

// badField is a field whose json tag encoding/json/v2 rejects.
type badField struct {
	typ reflect.Type // the struct declaring the field
	sf  reflect.StructField
	err error
}

// jsonName returns the member name of a field under the encoding/json/v2 tag
// grammar, whether the tag named it explicitly, and whether the field is
// skipped. The HTTP decoder and the canonical hasher use json/v2, so the
// validator must see exactly the names they see (G13): json:"-" skips the
// field; otherwise the name runs from the start of the tag to the first
// comma and may not contain a comma, backslash, or quote (json/v2 as shipped
// in Go 1.27 accepts no quoted names, so a member literally named "-" is
// spelled json:"-,omitempty"); options after the first comma are ignored,
// except that a trailing comma or an empty option is malformed. A tag json/v2
// rejects, including any tag other than "-" on an unexported field, is
// returned as an error so Compile fails naming the field.
func jsonName(sf reflect.StructField) (name string, explicit, skip bool, err error) {
	tag, hasTag := sf.Tag.Lookup("json")
	if tag == "-" {
		return "", false, true, nil
	}
	if hasTag && !sf.IsExported() && !sf.Anonymous {
		return "", false, false, fmt.Errorf("unexported field has json tag %q, which encoding/json/v2 rejects; remove the tag or use json:\"-\"", tag)
	}
	name, rest := sf.Name, tag
	if tag != "" && tag[0] != ',' {
		n := strings.IndexAny(tag, ",\\'\"`")
		if n < 0 {
			return tag, true, false, nil
		}
		if tag[n] != ',' {
			return "", false, false, fmt.Errorf("json tag %q is malformed: %q cannot appear in a member name (encoding/json/v2 rejects it)", tag, tag[n])
		}
		name, explicit, rest = tag[:n], true, tag[n:]
	}
	for rest != "" {
		opt, more, found := strings.Cut(rest[1:], ",")
		if opt == "" {
			what := "trailing comma"
			if found {
				what = "empty option"
			}
			return "", false, false, fmt.Errorf("json tag %q is malformed: %s (encoding/json/v2 rejects it)", tag, what)
		}
		rest = ""
		if found {
			rest = "," + more
		}
	}
	return name, explicit, false, nil
}

// collectFields flattens t the way encoding/json does: embedded structs
// without a JSON name are promoted (unexported ones too, but not unexported
// embedded pointers), an embedded struct with a JSON name is an ordinary
// member, the dominant field wins for repeated names, and marker, json:"-",
// and unexported fields are dropped. Fields whose json tag json/v2 rejects
// are returned separately in bad and take no part in dominance.
func collectFields(t reflect.Type) (flat []rawField, bad []badField) {
	type item struct {
		t     reflect.Type
		index []int
	}
	var (
		out   []rawField
		queue = []item{{t: t}}
		// visited records the depth at which a type was first walked: a type
		// met again deeper is dominated anyway (and this ends self-embedding),
		// while one met again at the same depth is walked so that the
		// dominance rule annihilates its fields like encoding/json does.
		visited = map[reflect.Type]int{}
	)
	for len(queue) > 0 {
		it := queue[0]
		queue = queue[1:]
		if depth, seen := visited[it.t]; seen && depth < len(it.index) {
			continue
		}
		visited[it.t] = len(it.index)
		for i := 0; i < it.t.NumField(); i++ {
			sf := it.t.Field(i)
			name, explicit, skip, err := jsonName(sf)
			if err != nil {
				bad = append(bad, badField{typ: it.t, sf: sf, err: err})
				continue
			}
			if skip {
				continue
			}
			index := append(slices.Clone(it.index), i)
			if sf.Anonymous {
				et := sf.Type
				if et.Kind() == reflect.Pointer {
					et = et.Elem()
				}
				if mediator.IsMarker(et) {
					continue
				}
				if !sf.IsExported() && (et.Kind() != reflect.Struct || sf.Type.Kind() == reflect.Pointer) {
					continue
				}
				if !explicit && et.Kind() == reflect.Struct && !isJSONMarshaler(et) && !isTextMarshaler(et) {
					queue = append(queue, item{t: et, index: index})
					continue
				}
			} else if !sf.IsExported() {
				continue
			}
			out = append(out, rawField{sf: sf, index: index, name: name, hasName: explicit})
		}
	}
	// Dominance: for each name keep the field that is alone at the shallowest
	// depth, or the only tagged one at that depth.
	slices.SortStableFunc(out, func(a, b rawField) int {
		if c := strings.Compare(a.name, b.name); c != 0 {
			return c
		}
		if c := len(a.index) - len(b.index); c != 0 {
			return c
		}
		return boolCompare(b.hasName, a.hasName)
	})
	for i := 0; i < len(out); {
		j := i + 1
		for j < len(out) && out[j].name == out[i].name {
			j++
		}
		if j-i == 1 || len(out[i].index) != len(out[i+1].index) || out[i].hasName != out[i+1].hasName {
			flat = append(flat, out[i])
		}
		i = j
	}
	slices.SortFunc(flat, func(a, b rawField) int { return slices.Compare(a.index, b.index) })
	return flat, bad
}

func boolCompare(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	}
	return -1
}

// typeName names a type in compile errors.
func typeName(t reflect.Type) string {
	if t.Name() == "" {
		return t.String()
	}
	return t.Name()
}

// compiler builds plans for one Compile session on a private copy of the
// plan map; the copy is published only when no error occurred.
type compiler struct {
	v     *Validator
	plans map[reflect.Type]*structPlan
	fresh []*structPlan
	errs  []error
}

func (c *compiler) errorf(where, format string, args ...any) {
	c.errs = append(c.errs, fmt.Errorf("validate: "+where+": "+format, args...))
}

// finish computes the transitive hasValidate flag of the new plans (a fixed
// point, because plans may be mutually recursive) and returns the joined
// errors of the session.
func (c *compiler) finish() error {
	for changed := true; changed; {
		changed = false
		for _, p := range c.fresh {
			if p.hasValidate {
				continue
			}
			hv := p.ownMode != validateNone
			for i := range p.fields {
				if p.fields[i].c != nil && p.fields[i].c.needsValidate() {
					hv = true
					break
				}
			}
			if hv {
				p.hasValidate, p.hasCheck, changed = true, true, true
			}
		}
	}
	return errors.Join(c.errs...)
}

// structPlan returns the plan of t, building it on first use. A placeholder
// is registered before the fields are walked so recursive types terminate.
func (c *compiler) structPlan(t reflect.Type) *structPlan {
	if p, ok := c.plans[t]; ok {
		return p
	}
	p := &structPlan{typ: t}
	c.plans[t] = p
	c.fresh = append(c.fresh, p)
	tname := typeName(t)
	fields, bad := collectFields(t)
	for _, b := range bad {
		c.errorf(typeName(b.typ)+"."+b.sf.Name, "%v", b.err)
	}
	for _, rf := range fields {
		where := tname + "." + rf.sf.Name
		if rf.sf.Anonymous && !rf.sf.IsExported() && validateModeOf(rf.sf.Type) != validateNone {
			// The value is reachable only through an unexported field, which
			// reflect refuses to hand out as an interface.
			c.errorf(where, "unexported embedded field with a JSON name implements Validate and cannot be called; export the type")
			continue
		}
		rules, err := parseTag(rf.sf.Tag.Get(c.v.tagKey))
		if err != nil {
			c.errorf(where, "%v", err)
			continue
		}
		fp := fieldPlan{
			name:   rf.name,
			seg:    escapePointer(rf.name),
			index:  rf.index,
			single: -1,
			typ:    rf.sf.Type,
			sf:     rf.sf,
			doc:    rf.sf.Tag.Get("doc"),
		}
		if len(rf.index) == 1 {
			fp.single = rf.index[0]
		}
		fp.c = c.checker(rf.sf.Type, rules, where)
		if fp.c != nil {
			p.hasCheck = true
		}
		p.fields = append(p.fields, fp)
	}
	p.ownMode = validateModeOf(t)
	if p.ownMode != validateNone {
		// Known before done so that a field of this type keeps its checker
		// even when no tag rule exists; finish propagates it transitively.
		p.hasValidate, p.hasCheck = true, true
	}
	p.done = true
	return p
}

// allowed rule sets per kind family.
var (
	allowPresence = []ruleKind{ruleRequired}
	allowString   = []ruleKind{ruleRequired, ruleAllowEmpty, ruleMin, ruleMax, ruleLen, rulePattern, ruleOneOf, ruleEmail, ruleUUID, ruleURL, ruleDateTime, ruleIPv4, ruleIPv6, ruleHostname}
	allowInt      = []ruleKind{ruleRequired, ruleMin, ruleMax, ruleGT, ruleGTE, ruleLT, ruleLTE, ruleOneOf}
	allowFloat    = []ruleKind{ruleRequired, ruleMin, ruleMax, ruleGT, ruleGTE, ruleLT, ruleLTE}
	allowSlice    = []ruleKind{ruleRequired, ruleMin, ruleMax, ruleLen, ruleUnique}
	allowMap      = []ruleKind{ruleRequired, ruleMin, ruleMax}
)

// allow reports every rule of own that is not in allowed as an error.
func (c *compiler) allow(own []rule, allowed []ruleKind, t reflect.Type, where string) bool {
	ok := true
	for _, r := range own {
		if !slices.Contains(allowed, r.kind) {
			c.errorf(where, "rule %q is not supported on %s", r.name(), t)
			ok = false
		}
	}
	return ok
}

// noDive reports dive on a type without elements.
func (c *compiler) noDive(dived bool, t reflect.Type, where string) {
	if dived {
		c.errorf(where, "rule \"dive\" is not supported on %s", t)
	}
}

// checker compiles rules for a value of type t. It returns nil when there is
// nothing to check at or below the value.
func (c *compiler) checker(t reflect.Type, rules []rule, where string) checker {
	own, rest, dived := splitDive(rules)
	req := hasRule(own, ruleRequired)
	// Pointers are unwrapped before anything looks at method sets, so that
	// *time.Time or a pointer to a marshaler still gets its nil check.
	if t.Kind() == reflect.Pointer {
		el := c.checker(t.Elem(), rules, where)
		if el == nil && !req {
			return nil
		}
		return &ptrChecker{req: req, el: el}
	}
	if isOpaque(t) {
		c.allow(own, allowPresence, t, where)
		c.noDive(dived, t, where)
		return presenceIf(req)
	}
	switch t.Kind() {
	case reflect.String:
		c.noDive(dived, t, where)
		return c.stringChecker(own, t, where)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		c.noDive(dived, t, where)
		bits := t.Bits()
		return compileNum(c, own, allowInt, t, where,
			func(s string) (int64, error) { return strconv.ParseInt(s, 10, bits) },
			reflect.Value.Int)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		c.noDive(dived, t, where)
		bits := t.Bits()
		return compileNum(c, own, allowInt, t, where,
			func(s string) (uint64, error) { return strconv.ParseUint(s, 10, bits) },
			reflect.Value.Uint)
	case reflect.Float32, reflect.Float64:
		c.noDive(dived, t, where)
		bits := t.Bits()
		return compileNum(c, own, allowFloat, t, where,
			func(s string) (float64, error) { return strconv.ParseFloat(s, bits) },
			reflect.Value.Float)
	case reflect.Slice, reflect.Array:
		if isBytes(t) {
			c.allow(own, allowPresence, t, where)
			c.noDive(dived, t, where)
			if !req || t.Kind() == reflect.Array {
				return presenceIf(req && t.Kind() == reflect.Array)
			}
			return &sliceChecker{req: true, minItems: -1, maxItems: -1, exact: -1}
		}
		el := c.checker(t.Elem(), rest, where)
		return c.sliceChecker(own, t, where, el)
	case reflect.Map:
		switch t.Key().Kind() {
		case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		default:
			c.errorf(where, "map key type %s is not supported; JSON object members are strings", t.Key())
			return nil
		}
		el := c.checker(t.Elem(), rest, where)
		return c.mapChecker(own, t, where, el)
	case reflect.Struct:
		c.allow(own, allowPresence, t, where)
		c.noDive(dived, t, where)
		p := c.structPlan(t)
		if !req && p.done && !p.hasCheck {
			return nil
		}
		return &structChecker{req: req, plan: p}
	default:
		c.errorf(where, "type %s cannot be validated or encoded as JSON", t)
		return nil
	}
}

func presenceIf(req bool) checker {
	if req {
		return presenceChecker{req: true}
	}
	return nil
}

// length parses a non-negative length argument.
func (c *compiler) length(r rule, where string) (int, bool) {
	n, err := strconv.ParseInt(r.arg, 10, 64)
	if err != nil || n < 0 || n > 1<<31-1 {
		c.errorf(where, "rule %q: %q is not a non-negative integer", r.name(), r.arg)
		return 0, false
	}
	return int(n), true
}

func (c *compiler) stringChecker(own []rule, t reflect.Type, where string) checker {
	if !c.allow(own, allowString, t, where) {
		return nil
	}
	if len(own) == 0 {
		return nil
	}
	sc := &stringChecker{minLen: -1, maxLen: -1, exact: -1}
	for _, r := range own {
		switch r.kind {
		case ruleRequired:
			sc.req = true
		case ruleAllowEmpty:
			sc.allowEmpty = true
		case ruleMin:
			if n, ok := c.length(r, where); ok {
				sc.minLen = n
				sc.minMsg = "must be at least " + plural(n, "character", "characters")
			}
		case ruleMax:
			if n, ok := c.length(r, where); ok {
				sc.maxLen = n
				sc.maxMsg = "must be at most " + plural(n, "character", "characters")
			}
		case ruleLen:
			if n, ok := c.length(r, where); ok {
				sc.exact = n
				sc.exactMsg = "must be exactly " + plural(n, "character", "characters")
			}
		case rulePattern:
			re, err := regexp.Compile(r.arg)
			if err != nil {
				c.errorf(where, "rule \"pattern\": %v", err)
				continue
			}
			sc.pattern = re
			sc.patternMsg = "must match " + r.arg
		case ruleOneOf:
			sc.oneof = strings.Fields(r.arg)
			sc.oneofMsg = "must be one of: " + strings.Join(sc.oneof, ", ")
		default:
			sc.format = formats[r.kind]
			sc.formatRule = r.name()
		}
	}
	return sc
}

// compileNum compiles numeric rules for one representation T.
func compileNum[T number](c *compiler, own []rule, allowed []ruleKind, t reflect.Type, where string,
	parse func(string) (T, error), get func(reflect.Value) T) checker {
	if !c.allow(own, allowed, t, where) {
		return nil
	}
	nc := &numChecker[T]{get: get}
	set := func(r rule, flag uint8, dst *T, msg *string, verb string) {
		x, err := parse(r.arg)
		if err != nil {
			c.errorf(where, "rule %q: %q is not a valid %s", r.name(), r.arg, t.Kind())
			return
		}
		nc.has |= flag
		*dst = x
		*msg = verb + fmt.Sprint(x)
	}
	for _, r := range own {
		switch r.kind {
		case ruleRequired:
			nc.req = true
		case ruleMin:
			set(r, hasMin, &nc.min, &nc.minMsg, "must be at least ")
		case ruleMax:
			set(r, hasMax, &nc.max, &nc.maxMsg, "must be at most ")
		case ruleGT:
			set(r, hasGT, &nc.gt, &nc.gtMsg, "must be greater than ")
		case ruleGTE:
			set(r, hasGTE, &nc.gte, &nc.gteMsg, "must be at least ")
		case ruleLT:
			set(r, hasLT, &nc.lt, &nc.ltMsg, "must be less than ")
		case ruleLTE:
			set(r, hasLTE, &nc.lte, &nc.lteMsg, "must be at most ")
		default: // oneof
			words := strings.Fields(r.arg)
			vals := make([]T, 0, len(words))
			for _, w := range words {
				x, err := parse(w)
				if err != nil {
					c.errorf(where, "rule \"oneof\": %q is not a valid %s", w, t.Kind())
					continue
				}
				vals = append(vals, x)
			}
			nc.oneof = vals
			nc.oneofMsg = "must be one of: " + strings.Join(words, ", ")
		}
	}
	if !nc.req && nc.has == 0 && nc.oneof == nil {
		return nil
	}
	return nc
}

func (c *compiler) sliceChecker(own []rule, t reflect.Type, where string, el checker) checker {
	if !c.allow(own, allowSlice, t, where) {
		return nil
	}
	sc := &sliceChecker{array: t.Kind() == reflect.Array, minItems: -1, maxItems: -1, exact: -1, el: el}
	for _, r := range own {
		switch r.kind {
		case ruleRequired:
			sc.req = true
		case ruleMin:
			if n, ok := c.length(r, where); ok {
				sc.minItems = n
				sc.minMsg = "must have at least " + plural(n, "item", "items")
			}
		case ruleMax:
			if n, ok := c.length(r, where); ok {
				sc.maxItems = n
				sc.maxMsg = "must have at most " + plural(n, "item", "items")
			}
		case ruleLen:
			if n, ok := c.length(r, where); ok {
				sc.exact = n
				sc.exactMsg = "must have exactly " + plural(n, "item", "items")
			}
		default: // unique
			if !isScalar(t.Elem()) {
				c.errorf(where, "rule \"unique\" requires a slice of scalars, not %s", t)
				continue
			}
			sc.unique = true
			sc.uniqMsg = "must not contain duplicates"
		}
	}
	if len(own) == 0 && el == nil {
		return nil
	}
	return sc
}

// isScalar reports whether values of t are comparable JSON scalars.
func isScalar(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func (c *compiler) mapChecker(own []rule, t reflect.Type, where string, el checker) checker {
	if !c.allow(own, allowMap, t, where) {
		return nil
	}
	mc := &mapChecker{minProps: -1, maxProps: -1, el: el}
	for _, r := range own {
		switch r.kind {
		case ruleRequired:
			mc.req = true
		case ruleMin:
			if n, ok := c.length(r, where); ok {
				mc.minProps = n
				mc.minMsg = "must have at least " + plural(n, "entry", "entries")
			}
		default: // max
			if n, ok := c.length(r, where); ok {
				mc.maxProps = n
				mc.maxMsg = "must have at most " + plural(n, "entry", "entries")
			}
		}
	}
	if len(own) == 0 && el == nil {
		return nil
	}
	return mc
}
