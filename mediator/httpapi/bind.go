package httpapi

import (
	"encoding"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/t3stackcoder/go-api-backend/mediator"
)

var uuidT = reflect.TypeFor[uuid.UUID]()

// bindParams populates the path, query, and header fields of req (a pointer
// to the request struct) from r. Every conversion failure becomes a field
// error at /<jsonName>; the result is nil or one *mediator.ValidationError.
func bindParams(req any, rt *route, r *http.Request) error {
	if len(rt.params) == 0 {
		return nil
	}
	v := reflect.ValueOf(req).Elem()
	query := r.URL.Query()
	var ve *mediator.ValidationError
	for _, b := range rt.params {
		var values []string
		switch b.Source {
		case SourcePath:
			values = []string{r.PathValue(b.Name)}
		case SourceQuery:
			values = query[b.Name]
		default:
			values = r.Header.Values(b.Name)
		}
		if len(values) == 0 {
			continue
		}
		if err := setFromStrings(fieldByIndexAlloc(v, b.Field.Index), values); err != nil {
			if ve == nil {
				ve = &mediator.ValidationError{}
			}
			ve.Add("/"+escapePointer(b.JSONName), "type", err.Error())
		}
	}
	if ve == nil {
		return nil
	}
	return ve
}

// fieldByIndexAlloc is Value.FieldByIndex that allocates nil embedded
// pointers along the way.
func fieldByIndexAlloc(v reflect.Value, index []int) reflect.Value {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

// setFromStrings converts values into fv. Slices take every value, each
// split on commas; scalars take the first value. An empty string is treated
// as absent for every type but string.
func setFromStrings(fv reflect.Value, values []string) error {
	t := fv.Type()
	if t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 {
		var parts []string
		for _, v := range values {
			for _, p := range strings.Split(v, ",") {
				if p = strings.TrimSpace(p); p != "" {
					parts = append(parts, p)
				}
			}
		}
		out := reflect.MakeSlice(t, len(parts), len(parts))
		for i, p := range parts {
			if err := setScalar(out.Index(i), p); err != nil {
				return fmt.Errorf("element %d %w", i, err)
			}
		}
		fv.Set(out)
		return nil
	}
	s := values[0]
	base := t
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if s == "" && base.Kind() != reflect.String {
		return nil
	}
	return setScalar(fv, s)
}

// setScalar parses s into fv according to its type.
func setScalar(fv reflect.Value, s string) error {
	t := fv.Type()
	if t.Kind() == reflect.Pointer {
		p := reflect.New(t.Elem())
		if err := setScalar(p.Elem(), s); err != nil {
			return err
		}
		fv.Set(p)
		return nil
	}
	switch t {
	case timeT:
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return errors.New("must be an RFC 3339 date-time")
		}
		fv.Set(reflect.ValueOf(ts))
		return nil
	case uuidT:
		id, err := uuid.Parse(s)
		if err != nil {
			return errors.New("must be a UUID")
		}
		fv.Set(reflect.ValueOf(id))
		return nil
	}
	if fv.CanAddr() {
		if u, ok := fv.Addr().Interface().(encoding.TextUnmarshaler); ok {
			if err := u.UnmarshalText([]byte(s)); err != nil {
				return fmt.Errorf("is invalid: %w", err)
			}
			return nil
		}
	}
	switch t.Kind() {
	case reflect.String:
		fv.SetString(s)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return errors.New("must be a boolean")
		}
		fv.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, t.Bits())
		if err != nil {
			return intError(err, t.Bits(), true)
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, t.Bits())
		if err != nil {
			return intError(err, t.Bits(), false)
		}
		fv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(s, t.Bits())
		if err != nil {
			return errors.New("must be a number")
		}
		fv.SetFloat(f)
	default:
		return fmt.Errorf("cannot be bound from a string (%s)", t)
	}
	return nil
}

func intError(err error, bits int, signed bool) error {
	if errors.Is(err, strconv.ErrRange) {
		if signed {
			return fmt.Errorf("must fit in a signed %d-bit integer", bits)
		}
		return fmt.Errorf("must fit in an unsigned %d-bit integer", bits)
	}
	if signed {
		return errors.New("must be an integer")
	}
	return errors.New("must be a non-negative integer")
}

// escapePointer escapes one JSON Pointer reference token (RFC 6901).
func escapePointer(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}
