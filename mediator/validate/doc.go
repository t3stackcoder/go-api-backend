// Package validate is the request validator of the mediator framework. One
// interpreter of the closed struct tag grammar (spec 5.8) produces both the
// run-time checks run by the Validation behavior and the JSON Schema
// constraints emitted into the OpenAPI document, so clients see exactly the
// rules the server enforces.
//
// # Tag grammar
//
// Rules live in the `validate` struct tag, separated by commas:
//
//	type Line struct {
//	    SKU   string  `json:"sku"   validate:"required,pattern=^[A-Z0-9-]{3,32}$"`
//	    Qty   int     `json:"qty"   validate:"required,min=1,max=1000"`
//	    Price float64 `json:"price" validate:"gte=0"`
//	    Note  *string `json:"note"  validate:"max=200"`
//	    Tags  []string `json:"tags" validate:"max=10,unique,dive,min=1"`
//	}
//
// The grammar is closed: required, allowempty, min=n, max=n, gt=n, gte=n,
// lt=n, lte=n, len=n, pattern=re, oneof=a b c, email, uuid, url, datetime,
// ipv4, ipv6, hostname, unique, and dive. An unknown rule, a rule applied to a
// type that does not support it, or a malformed value is a Compile error (and
// therefore a Build error), never a silent no-op. pattern must be the last
// rule: its value runs to the end of the tag so it may contain commas.
//
// # Semantics
//
// Field paths in errors are JSON Pointers built from json tag names, for
// example /lines/0/qty. Nested structs are validated recursively, embedded
// structs are flattened like encoding/json does, marker fields and json:"-"
// fields are skipped. Member names follow the encoding/json/v2 tag grammar
// exactly, because that is what the HTTP decoder and the canonical hasher
// use: the name runs to the first comma and may not contain a comma,
// backslash, or quote, options after it are ignored, and a tag json/v2
// rejects (a trailing comma, an empty option, a quoted name, or any tag
// other than "-" on an unexported field) is a Compile error naming the field.
//
// required means "present and not null" in the JSON document. Pointers,
// slices, maps and []byte fail required when nil; strings fail it when empty
// unless allowempty is set. On non-nilable kinds (numbers, booleans, structs,
// time.Time, uuid.UUID) a decoded value is always present, so required only
// lists the property in the schema; use min, gt, or a pointer to reject the
// zero value.
//
// A nil pointer, slice, or map that is not required is an absent optional
// value: no other rule runs on it, exactly as a JSON Schema imposes nothing
// on a missing property. The generated schema marks such fields nullable.
//
// After every tag rule passes, Validate(ctx) error is called on every nested
// struct (depth first) and finally on the root when the type implements
// mediator.Validator. A returned *mediator.ValidationError is merged with the
// nested path prefixed; any other error rejects the whole request as-is. An
// embedded struct's Validate takes part through Go method promotion: it is
// the outer struct's Validate unless the outer declares its own, which then
// decides whether to call it.
//
// # JSON Schema
//
// SchemaFor maps a Go type to a JSON Schema 2020-12 subset following the
// table in spec 5.8 and the type mapping in spec 8.6. Named struct types are
// registered in a Schemas registry and referenced with $ref; anonymous
// structs are inlined. Schema marshals with encoding/json/v2; properties are
// emitted in field order, while the $defs and Extra maps follow the encoder's
// map order, so pass json.Deterministic(true) when byte-stable output matters.
package validate
