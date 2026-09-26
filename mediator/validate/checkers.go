package validate

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// checkCtx is the per-call state of Check: the current JSON Pointer path as
// a stack of escaped segments and the failures collected so far.
type checkCtx struct {
	path []string
	errs []mediator.FieldError
	max  int
}

func (c *checkCtx) push(seg string) { c.path = append(c.path, seg) }
func (c *checkCtx) pop()            { c.path = c.path[:len(c.path)-1] }

// pointer renders the current path as a JSON Pointer ("" at the root).
func (c *checkCtx) pointer() string {
	if len(c.path) == 0 {
		return ""
	}
	var b strings.Builder
	for _, s := range c.path {
		b.WriteByte('/')
		b.WriteString(s)
	}
	return b.String()
}

// fail records one failure at the current path unless the cap is reached.
func (c *checkCtx) fail(rule, msg string) {
	if c.max > 0 && len(c.errs) >= c.max {
		return
	}
	c.errs = append(c.errs, mediator.FieldError{Path: c.pointer(), Rule: rule, Message: msg})
}

// runValidate calls the Validate method of v. A *mediator.ValidationError is
// merged with the current path as prefix; any other error is returned.
func (c *checkCtx) runValidate(ctx context.Context, v reflect.Value, mode validateMode) error {
	err := callValidate(ctx, v, mode)
	if err == nil {
		return nil
	}
	var ve *mediator.ValidationError
	if !errors.As(err, &ve) {
		return err
	}
	prefix := c.pointer()
	for _, f := range ve.Fields {
		if c.max > 0 && len(c.errs) >= c.max {
			break
		}
		c.errs = append(c.errs, mediator.FieldError{Path: prefix + f.Path, Rule: f.Rule, Message: f.Message})
	}
	return nil
}

// validateMode records how a struct type implements mediator.Validator.
type validateMode uint8

const (
	validateNone  validateMode = iota
	validateValue              // value receiver
	validatePtr                // pointer receiver
)

var validatorType = reflect.TypeFor[mediator.Validator]()

func validateModeOf(t reflect.Type) validateMode {
	switch {
	case t.Implements(validatorType):
		return validateValue
	case reflect.PointerTo(t).Implements(validatorType):
		return validatePtr
	}
	return validateNone
}

// callValidate invokes Validate on v without copying when v is addressable
// (the pointer method set includes value methods); a non-addressable value
// with a pointer receiver is copied once.
func callValidate(ctx context.Context, v reflect.Value, mode validateMode) error {
	var target any
	switch {
	case v.CanAddr():
		target = v.Addr().Interface()
	case mode == validateValue:
		target = v.Interface()
	default:
		cp := reflect.New(v.Type())
		cp.Elem().Set(v)
		target = cp.Interface()
	}
	return target.(mediator.Validator).Validate(ctx)
}

// escapePointer escapes one JSON Pointer reference token (RFC 6901).
func escapePointer(seg string) string {
	if !strings.ContainsAny(seg, "~/") {
		return seg
	}
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(seg)
}

// plural renders "1 item" / "2 items" with an irregular plural form.
func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}

// checker is one compiled rule set for one Go type. Implementations are
// immutable after compile and safe for concurrent use.
type checker interface {
	// check runs the tag rules on v, which is a valid value of the checker's type.
	check(cc *checkCtx, v reflect.Value)
	// validate runs Validate methods on the structs below v, depth first.
	validate(ctx context.Context, cc *checkCtx, v reflect.Value) error
	// needsValidate reports whether any struct below implements Validate.
	needsValidate() bool
	// required reports whether the rule set includes required.
	required() bool
	// constrain adds the JSON Schema keywords of the rule set to s.
	constrain(s *Schema)
}

// presenceChecker is the checker of kinds on which required only lists the
// property in the schema (bool, interfaces, time.Time, uuid.UUID, opaque
// marshalers, byte arrays).
type presenceChecker struct{ req bool }

func (presenceChecker) check(*checkCtx, reflect.Value)                           {}
func (presenceChecker) validate(context.Context, *checkCtx, reflect.Value) error { return nil }
func (presenceChecker) needsValidate() bool                                      { return false }
func (p presenceChecker) required() bool                                         { return p.req }
func (presenceChecker) constrain(*Schema)                                        {}

// stringChecker holds the rules of a string kind.
type stringChecker struct {
	req, allowEmpty bool
	minLen, maxLen  int // -1 when unset
	exact           int // len=n, -1 when unset
	pattern         *regexp.Regexp
	oneof           []string
	format          formatSpec
	formatRule      string // rule name of the format, "" when none

	minMsg, maxMsg, exactMsg, patternMsg, oneofMsg string
}

func (c *stringChecker) check(cc *checkCtx, v reflect.Value) {
	s := v.String()
	if s == "" && c.req && !c.allowEmpty {
		cc.fail("required", "is required")
		return
	}
	n := utf8.RuneCountInString(s)
	if c.exact >= 0 && n != c.exact {
		cc.fail("len", c.exactMsg)
	}
	if c.minLen >= 0 && n < c.minLen {
		cc.fail("min", c.minMsg)
	}
	if c.maxLen >= 0 && n > c.maxLen {
		cc.fail("max", c.maxMsg)
	}
	if c.pattern != nil && !c.pattern.MatchString(s) {
		cc.fail("pattern", c.patternMsg)
	}
	if c.oneof != nil && !slices.Contains(c.oneof, s) {
		cc.fail("oneof", c.oneofMsg)
	}
	if c.format.check != nil && !c.format.check(s) {
		cc.fail(c.formatRule, c.format.msg)
	}
}

func (*stringChecker) validate(context.Context, *checkCtx, reflect.Value) error { return nil }
func (*stringChecker) needsValidate() bool                                      { return false }
func (c *stringChecker) required() bool                                         { return c.req }

func (c *stringChecker) constrain(s *Schema) {
	lo, hi := c.minLen, c.maxLen
	if c.req && !c.allowEmpty {
		lo = max(lo, 1)
	}
	if c.exact >= 0 {
		lo = max(lo, c.exact)
		if hi < 0 {
			hi = c.exact
		} else {
			hi = min(hi, c.exact)
		}
	}
	if lo >= 0 {
		s.MinLength = ptr(lo)
	}
	if hi >= 0 {
		s.MaxLength = ptr(hi)
	}
	if c.pattern != nil {
		s.Pattern = c.pattern.String()
	}
	if c.oneof != nil {
		s.Enum = make([]any, len(c.oneof))
		for i, o := range c.oneof {
			s.Enum[i] = o
		}
	}
	if c.format.schema != "" {
		s.Format = c.format.schema
	}
}

// number is the set of representations numeric kinds are checked in.
type number interface{ ~int64 | ~uint64 | ~float64 }

// Bound flags of numChecker.has.
const (
	hasMin uint8 = 1 << iota
	hasMax
	hasGT
	hasGTE
	hasLT
	hasLTE
)

// numChecker holds the rules of an integer, unsigned, or float kind, read
// through get (reflect.Value.Int, Uint, or Float).
type numChecker[T number] struct {
	req                        bool
	has                        uint8
	min, max, gt, gte, lt, lte T
	oneof                      []T
	get                        func(reflect.Value) T

	minMsg, maxMsg, gtMsg, gteMsg, ltMsg, lteMsg, oneofMsg string
}

func (c *numChecker[T]) check(cc *checkCtx, v reflect.Value) {
	x := c.get(v)
	if c.has&hasMin != 0 && x < c.min {
		cc.fail("min", c.minMsg)
	}
	if c.has&hasMax != 0 && x > c.max {
		cc.fail("max", c.maxMsg)
	}
	if c.has&hasGT != 0 && x <= c.gt {
		cc.fail("gt", c.gtMsg)
	}
	if c.has&hasGTE != 0 && x < c.gte {
		cc.fail("gte", c.gteMsg)
	}
	if c.has&hasLT != 0 && x >= c.lt {
		cc.fail("lt", c.ltMsg)
	}
	if c.has&hasLTE != 0 && x > c.lte {
		cc.fail("lte", c.lteMsg)
	}
	if c.oneof != nil && !slices.Contains(c.oneof, x) {
		cc.fail("oneof", c.oneofMsg)
	}
}

func (*numChecker[T]) validate(context.Context, *checkCtx, reflect.Value) error { return nil }
func (*numChecker[T]) needsValidate() bool                                      { return false }
func (c *numChecker[T]) required() bool                                         { return c.req }

func (c *numChecker[T]) constrain(s *Schema) {
	switch {
	case c.has&hasMin != 0 && c.has&hasGTE != 0:
		s.Minimum = ptr(float64(max(c.min, c.gte)))
	case c.has&hasMin != 0:
		s.Minimum = ptr(float64(c.min))
	case c.has&hasGTE != 0:
		s.Minimum = ptr(float64(c.gte))
	}
	switch {
	case c.has&hasMax != 0 && c.has&hasLTE != 0:
		s.Maximum = ptr(float64(min(c.max, c.lte)))
	case c.has&hasMax != 0:
		s.Maximum = ptr(float64(c.max))
	case c.has&hasLTE != 0:
		s.Maximum = ptr(float64(c.lte))
	}
	if c.has&hasGT != 0 {
		s.ExclusiveMinimum = ptr(float64(c.gt))
	}
	if c.has&hasLT != 0 {
		s.ExclusiveMaximum = ptr(float64(c.lt))
	}
	if c.oneof != nil {
		s.Enum = make([]any, len(c.oneof))
		for i, o := range c.oneof {
			s.Enum[i] = o
		}
	}
}

// ptrChecker unwraps a pointer. nil fails required and otherwise skips every
// rule; the element checker carries the rules (including required, which on
// a *string still means non-empty).
type ptrChecker struct {
	req bool
	el  checker // may be nil
}

func (c *ptrChecker) check(cc *checkCtx, v reflect.Value) {
	if v.IsNil() {
		if c.req {
			cc.fail("required", "is required")
		}
		return
	}
	if c.el != nil {
		c.el.check(cc, v.Elem())
	}
}

func (c *ptrChecker) validate(ctx context.Context, cc *checkCtx, v reflect.Value) error {
	if v.IsNil() {
		return nil
	}
	return c.el.validate(ctx, cc, v.Elem())
}

func (c *ptrChecker) needsValidate() bool { return c.el != nil && c.el.needsValidate() }
func (c *ptrChecker) required() bool      { return c.req }
func (*ptrChecker) constrain(*Schema)     {}

// sliceChecker holds the rules of a slice or array and the element checker
// compiled from the rules after dive (or from the element type alone).
type sliceChecker struct {
	req               bool
	array             bool // fixed-size array: never nil
	minItems          int  // -1 when unset
	maxItems          int
	exact             int
	unique            bool
	el                checker // may be nil
	minMsg, maxMsg    string
	exactMsg, uniqMsg string
}

func (c *sliceChecker) check(cc *checkCtx, v reflect.Value) {
	if !c.array && v.IsNil() {
		if c.req {
			cc.fail("required", "is required")
		}
		return
	}
	n := v.Len()
	if c.exact >= 0 && n != c.exact {
		cc.fail("len", c.exactMsg)
	}
	if c.minItems >= 0 && n < c.minItems {
		cc.fail("min", c.minMsg)
	}
	if c.maxItems >= 0 && n > c.maxItems {
		cc.fail("max", c.maxMsg)
	}
	if c.unique && !allUnique(v, n) {
		cc.fail("unique", c.uniqMsg)
	}
	if c.el != nil {
		for i := 0; i < n; i++ {
			cc.push(strconv.Itoa(i))
			c.el.check(cc, v.Index(i))
			cc.pop()
		}
	}
}

// allUnique reports whether the scalar elements of v are pairwise distinct.
func allUnique(v reflect.Value, n int) bool {
	seen := make(map[any]struct{}, n)
	for i := 0; i < n; i++ {
		k := v.Index(i).Interface()
		if _, dup := seen[k]; dup {
			return false
		}
		seen[k] = struct{}{}
	}
	return true
}

func (c *sliceChecker) validate(ctx context.Context, cc *checkCtx, v reflect.Value) error {
	if !c.array && v.IsNil() {
		return nil
	}
	for i, n := 0, v.Len(); i < n; i++ {
		cc.push(strconv.Itoa(i))
		err := c.el.validate(ctx, cc, v.Index(i))
		cc.pop()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *sliceChecker) needsValidate() bool { return c.el != nil && c.el.needsValidate() }
func (c *sliceChecker) required() bool      { return c.req }

func (c *sliceChecker) constrain(s *Schema) {
	lo, hi := c.minItems, c.maxItems
	if c.exact >= 0 {
		lo = max(lo, c.exact)
		if hi < 0 {
			hi = c.exact
		} else {
			hi = min(hi, c.exact)
		}
	}
	if lo >= 0 {
		s.MinItems = ptr(lo)
	}
	if hi >= 0 {
		s.MaxItems = ptr(hi)
	}
	s.UniqueItems = c.unique
}

// mapChecker holds the rules of a map and the checker of its values.
type mapChecker struct {
	req            bool
	minProps       int // -1 when unset
	maxProps       int
	el             checker // may be nil
	minMsg, maxMsg string
}

// mapEntry is one map entry with its escaped JSON Pointer segment.
type mapEntry struct {
	seg string
	key reflect.Value
}

// sortedEntries returns the entries of v ordered by key segment so that
// error order is deterministic.
func sortedEntries(v reflect.Value) []mapEntry {
	entries := make([]mapEntry, 0, v.Len())
	for it := v.MapRange(); it.Next(); {
		entries = append(entries, mapEntry{seg: escapePointer(keySegment(it.Key())), key: it.Key()})
	}
	slices.SortFunc(entries, func(a, b mapEntry) int { return strings.Compare(a.seg, b.seg) })
	return entries
}

// keySegment renders a map key the way encoding/json names the member.
func keySegment(k reflect.Value) string {
	switch k.Kind() {
	case reflect.String:
		return k.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10)
	default:
		return strconv.FormatUint(k.Uint(), 10)
	}
}

func (c *mapChecker) check(cc *checkCtx, v reflect.Value) {
	if v.IsNil() {
		if c.req {
			cc.fail("required", "is required")
		}
		return
	}
	n := v.Len()
	if c.minProps >= 0 && n < c.minProps {
		cc.fail("min", c.minMsg)
	}
	if c.maxProps >= 0 && n > c.maxProps {
		cc.fail("max", c.maxMsg)
	}
	if c.el != nil {
		for _, e := range sortedEntries(v) {
			cc.push(e.seg)
			c.el.check(cc, v.MapIndex(e.key))
			cc.pop()
		}
	}
}

func (c *mapChecker) validate(ctx context.Context, cc *checkCtx, v reflect.Value) error {
	if v.IsNil() {
		return nil
	}
	for _, e := range sortedEntries(v) {
		cc.push(e.seg)
		err := c.el.validate(ctx, cc, v.MapIndex(e.key))
		cc.pop()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *mapChecker) needsValidate() bool { return c.el != nil && c.el.needsValidate() }
func (c *mapChecker) required() bool      { return c.req }

func (c *mapChecker) constrain(s *Schema) {
	if c.minProps >= 0 {
		s.MinProperties = ptr(c.minProps)
	}
	if c.maxProps >= 0 {
		s.MaxProperties = ptr(c.maxProps)
	}
}

// structChecker runs the plan of a nested struct.
type structChecker struct {
	req  bool
	plan *structPlan
}

func (c *structChecker) check(cc *checkCtx, v reflect.Value) { c.plan.check(cc, v) }
func (c *structChecker) validate(ctx context.Context, cc *checkCtx, v reflect.Value) error {
	return c.plan.validate(ctx, cc, v)
}
func (c *structChecker) needsValidate() bool { return c.plan.hasValidate }
func (c *structChecker) required() bool      { return c.req }
func (*structChecker) constrain(*Schema)     {}

// elemOf returns the element checker of a pointer, slice, or map checker.
func elemOf(c checker) checker {
	switch x := c.(type) {
	case *ptrChecker:
		return x.el
	case *sliceChecker:
		return x.el
	case *mapChecker:
		return x.el
	}
	return nil
}

func ptr[T any](v T) *T { return &v }
