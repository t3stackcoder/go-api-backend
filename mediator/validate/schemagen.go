package validate

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// DefaultRefPrefix is the $ref prefix of registered schemas in an OpenAPI
// document.
const DefaultRefPrefix = "#/components/schemas/"

// SchemaOptions controls SchemaFor.
type SchemaOptions struct {
	// AllowUnknownFields omits additionalProperties: false from objects.
	AllowUnknownFields bool
	// SkipField excludes fields from the schema (the HTTP adapter uses it for
	// fields bound to path, query, or header parameters).
	SkipField func(reflect.StructField) bool
	// Descriptions copies doc:"..." struct tags into description keywords.
	Descriptions bool
	// RefPrefix is prepended to schema names in $ref; DefaultRefPrefix when
	// empty. Use "#/$defs/" for a standalone document whose Defs are inlined.
	RefPrefix string
}

// Schemas is the registry of named struct schemas. Defs holds the schema of
// every registered type by name; SchemaFor returns $ref pointers into it.
// Names are Go type names, qualified with the last package path element when
// two packages export the same name (both colliding types are qualified) and
// suffixed with a counter when even that collides. The zero value is ready.
type Schemas struct {
	Defs map[string]*Schema

	names  map[reflect.Type]string
	owners map[string]reflect.Type
	seen   map[string]bool
	refs   map[reflect.Type][]refStub
}

// refStub is one $ref schema handed out for a type; renames update it.
type refStub struct {
	s      *Schema
	prefix string
}

func (r *Schemas) init() {
	if r.Defs == nil {
		r.Defs = map[string]*Schema{}
	}
	if r.names == nil {
		r.names = map[reflect.Type]string{}
		r.owners = map[string]reflect.Type{}
		r.seen = map[string]bool{}
		r.refs = map[reflect.Type][]refStub{}
	}
}

// Names returns the registered names in sorted order.
func (r *Schemas) Names() []string { return slices.Sorted(maps.Keys(r.Defs)) }

// NameOf returns the registered name of t.
func (r *Schemas) NameOf(t reflect.Type) (string, bool) {
	n, ok := r.names[t]
	return n, ok
}

// shortName is the sanitized Go type name.
func shortName(t reflect.Type) string {
	return strings.Map(func(c rune) rune {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			return c
		}
		return '_'
	}, t.Name())
}

// qualifiedName prefixes the short name with the last package path element.
func qualifiedName(t reflect.Type) string {
	pkg := t.PkgPath()
	if i := strings.LastIndexByte(pkg, '/'); i >= 0 {
		pkg = pkg[i+1:]
	}
	return pkg + "." + shortName(t)
}

// unique appends a counter until name is free in Defs.
func (r *Schemas) unique(name string) string {
	out := name
	for i := 2; r.Defs[out] != nil; i++ {
		out = name + "_" + strconv.Itoa(i)
	}
	return out
}

// register reserves a name for t and reports whether it was new.
func (r *Schemas) register(t reflect.Type) (string, bool) {
	r.init()
	if n, ok := r.names[t]; ok {
		return n, false
	}
	short := shortName(t)
	name := short
	if other, taken := r.owners[short]; taken {
		r.rename(other, r.unique(qualifiedName(other)))
		name = r.unique(qualifiedName(t))
	} else if r.seen[short] || r.Defs[short] != nil {
		name = r.unique(qualifiedName(t))
	}
	r.seen[short] = true
	r.names[t] = name
	r.owners[name] = t
	r.Defs[name] = &Schema{}
	return name, true
}

// rename moves t to a new name and rewrites every $ref handed out for it.
func (r *Schemas) rename(t reflect.Type, name string) {
	old := r.names[t]
	s := r.Defs[old]
	delete(r.Defs, old)
	delete(r.owners, old)
	r.Defs[name] = s
	r.owners[name] = t
	r.names[t] = name
	for _, stub := range r.refs[t] {
		stub.s.Ref = stub.prefix + name
	}
}

// ref returns a new $ref schema for the registered type t.
func (r *Schemas) ref(t reflect.Type, prefix string) *Schema {
	s := &Schema{Ref: prefix + r.names[t]}
	r.refs[t] = append(r.refs[t], refStub{s: s, prefix: prefix})
	return s
}

// SchemaFor returns the JSON Schema of t. Named struct types are registered
// in reg and returned as a $ref; anonymous structs and every other type are
// returned inline. With a nil reg every struct is inlined, which fails for
// recursive types. Tag rules are compiled first, so a bad tag anywhere in the
// type graph is an error here too.
func (v *Validator) SchemaFor(t reflect.Type, reg *Schemas, opts SchemaOptions) (*Schema, error) {
	if t == nil {
		return nil, ErrNilValue
	}
	if opts.RefPrefix == "" {
		opts.RefPrefix = DefaultRefPrefix
	}
	g := &schemaGen{v: v, reg: reg, opts: opts, inline: map[reflect.Type]bool{}}
	return g.schemaOf(t, nil)
}

// schemaGen is the state of one SchemaFor call.
type schemaGen struct {
	v      *Validator
	reg    *Schemas
	opts   SchemaOptions
	inline map[reflect.Type]bool // structs being inlined, for cycle detection
}

// schemaOf maps t to a schema, with c (the compiled checker of the field
// holding t, possibly nil) supplying the constraints.
func (g *schemaGen) schemaOf(t reflect.Type, c checker) (*Schema, error) {
	if t.Kind() == reflect.Pointer {
		el, err := g.schemaOf(t.Elem(), elemOf(c))
		if err != nil {
			return nil, err
		}
		return nullableUnless(el, c), nil
	}
	if implementsEither(t, schemerType) {
		return g.custom(t)
	}
	switch t {
	case timeType:
		return &Schema{Type: Type{"string"}, Format: "date-time"}, nil
	case uuidType:
		return &Schema{Type: Type{"string"}, Format: "uuid"}, nil
	case rawType:
		return &Schema{}, nil
	}
	if isJSONMarshaler(t) {
		return &Schema{}, nil
	}
	if t.Kind() != reflect.String && isTextMarshaler(t) {
		return &Schema{Type: Type{"string"}}, nil
	}
	var s *Schema
	switch t.Kind() {
	case reflect.Bool:
		s = &Schema{Type: Type{"boolean"}}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		s = &Schema{Type: Type{"integer"}}
	case reflect.Float32, reflect.Float64:
		s = &Schema{Type: Type{"number"}}
	case reflect.String:
		s = &Schema{Type: Type{"string"}}
	case reflect.Interface:
		return &Schema{}, nil
	case reflect.Slice, reflect.Array:
		if isBytes(t) {
			s = &Schema{Type: Type{"string"}, ContentEncoding: "base64"}
		} else {
			items, err := g.schemaOf(t.Elem(), elemOf(c))
			if err != nil {
				return nil, err
			}
			s = &Schema{Type: Type{"array"}, Items: items}
		}
		if c != nil {
			c.constrain(s)
		}
		if t.Kind() == reflect.Slice {
			s = nullableUnless(s, c)
		}
		return s, nil
	case reflect.Map:
		values, err := g.schemaOf(t.Elem(), elemOf(c))
		if err != nil {
			return nil, err
		}
		s = &Schema{Type: Type{"object"}, AdditionalProperties: &AdditionalProperties{Schema: values}}
		if c != nil {
			c.constrain(s)
		}
		return nullableUnless(s, c), nil
	case reflect.Struct:
		return g.structSchema(t)
	default:
		return nil, fmt.Errorf("validate: type %s cannot be encoded as JSON", t)
	}
	if c != nil {
		c.constrain(s)
	}
	return s, nil
}

// custom returns the schema a Schemer supplies, registered when the type is
// a named struct. The returned schema is a shallow copy so callers may
// adjust it.
func (g *schemaGen) custom(t reflect.Type) (*Schema, error) {
	var s *Schema
	if t.Implements(schemerType) {
		s = reflect.Zero(t).Interface().(Schemer).JSONSchema()
	} else {
		s = reflect.New(t).Interface().(Schemer).JSONSchema()
	}
	if s == nil {
		return nil, fmt.Errorf("validate: %s.JSONSchema returned nil", t)
	}
	cp := *s
	cp.Type = slices.Clone(s.Type)
	cp.Enum = slices.Clone(s.Enum)
	if t.Kind() == reflect.Struct && t.Name() != "" && g.reg != nil {
		name, fresh := g.reg.register(t)
		if fresh {
			g.reg.Defs[name] = &cp
		}
		return g.reg.ref(t, g.opts.RefPrefix), nil
	}
	return &cp, nil
}

// structSchema registers a named struct and returns a $ref, or inlines an
// anonymous one.
func (g *schemaGen) structSchema(t reflect.Type) (*Schema, error) {
	p, err := g.v.plan(t)
	if err != nil {
		return nil, err
	}
	if t.Name() != "" && g.reg != nil {
		name, fresh := g.reg.register(t)
		if fresh {
			body, err := g.structBody(p)
			if err != nil {
				return nil, err
			}
			g.reg.Defs[name] = body
		}
		return g.reg.ref(t, g.opts.RefPrefix), nil
	}
	if g.inline[t] {
		return nil, fmt.Errorf("validate: recursive type %s needs a registry", t)
	}
	g.inline[t] = true
	defer delete(g.inline, t)
	return g.structBody(p)
}

// structBody builds the object schema of a plan.
func (g *schemaGen) structBody(p *structPlan) (*Schema, error) {
	s := &Schema{Type: Type{"object"}}
	if !g.opts.AllowUnknownFields {
		s.AdditionalProperties = &AdditionalProperties{}
	}
	var errs []error
	for i := range p.fields {
		f := &p.fields[i]
		if g.opts.SkipField != nil && g.opts.SkipField(f.sf) {
			continue
		}
		fs, err := g.schemaOf(f.typ, f.c)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if g.opts.Descriptions && f.doc != "" {
			fs.Description = f.doc
		}
		if f.c != nil && f.c.required() {
			s.Required = append(s.Required, f.name)
		}
		s.Properties = append(s.Properties, Property{Name: f.name, Schema: fs})
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return s, nil
}

// nullableUnless makes s accept null unless the field is required.
func nullableUnless(s *Schema, c checker) *Schema {
	if c != nil && c.required() {
		return s
	}
	return nullable(s)
}

// nullable returns a schema accepting null in addition to s: "null" is added
// to a typed schema (and to its enum), the empty schema already accepts it,
// and a $ref or composite schema is wrapped in anyOf.
func nullable(s *Schema) *Schema {
	if s.Ref == "" && len(s.AnyOf) == 0 {
		switch {
		case slices.Contains(s.Type, "null"):
			return s
		case len(s.Type) > 0:
			s.Type = append(s.Type, "null")
			if len(s.Enum) > 0 {
				s.Enum = append(s.Enum, nil)
			}
			return s
		case isEmptySchema(s):
			return s
		}
	}
	return &Schema{AnyOf: []*Schema{s, {Type: Type{"null"}}}}
}
