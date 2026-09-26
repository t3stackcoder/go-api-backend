package history

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

// Normalize returns a canonical form of a history value so that a value
// recorded in memory (int, int64, a struct) compares equal to the same value
// read back from JSON Lines (float64, map[string]any). Integral numbers
// become int64, other numbers float64, slices []any, structs and maps
// map[string]any with normalized members; strings, bools, and nil are
// returned as is.
func Normalize(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return x
	case bool:
		return x
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case float32:
		return normFloat(float64(x))
	case float64:
		return normFloat(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = Normalize(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = Normalize(e)
		}
		return out
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return Normalize(rv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return []any{}
		}
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = Normalize(rv.Index(i).Interface())
		}
		return out
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[fmt.Sprint(iter.Key().Interface())] = Normalize(iter.Value().Interface())
		}
		return out
	case reflect.Struct:
		out := map[string]any{}
		rt := rv.Type()
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}
			name := f.Name
			if tag := f.Tag.Get("json"); tag != "" {
				if n, _, _ := strings.Cut(tag, ","); n == "-" {
					continue
				} else if n != "" {
					name = n
				}
			}
			out[name] = Normalize(rv.Field(i).Interface())
		}
		return out
	default:
		return fmt.Sprint(v)
	}
}

func normFloat(f float64) any {
	if f == math.Trunc(f) && math.Abs(f) < (1<<62) {
		return int64(f)
	}
	return f
}

// Equal reports whether two history values are equal after normalization.
func Equal(a, b any) bool { return reflect.DeepEqual(Normalize(a), Normalize(b)) }

// Int converts a normalized number to int64. It accepts every integer kind,
// integral floats, and numeric strings.
func Int(v any) (int64, bool) {
	switch x := Normalize(v).(type) {
	case int64:
		return x, true
	case float64:
		return 0, false
	case string:
		var n int64
		if _, err := fmt.Sscan(x, &n); err == nil {
			return n, true
		}
	}
	return 0, false
}

// Ints converts a normalized list to []int64. It accepts nil (an empty list),
// []int64, []int, []any, and []float64.
func Ints(v any) ([]int64, bool) {
	if v == nil {
		return []int64{}, true
	}
	list, ok := Normalize(v).([]any)
	if !ok {
		return nil, false
	}
	out := make([]int64, len(list))
	for i, e := range list {
		n, ok := Int(e)
		if !ok {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

// Strings converts a normalized list of strings, as stored in Op.Extra
// (for example the "tags" of a cached read), to []string.
func Strings(v any) []string {
	switch x := v.(type) {
	case nil:
		return nil
	case []string:
		return x
	case string:
		return []string{x}
	}
	list, ok := Normalize(v).([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		out = append(out, fmt.Sprint(e))
	}
	return out
}

// Describe renders a value for reports and visualizations.
func Describe(v any) string {
	switch x := Normalize(v).(type) {
	case nil:
		return "nil"
	case string:
		return fmt.Sprintf("%q", x)
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = k + ":" + Describe(x[k])
		}
		return "{" + strings.Join(parts, " ") + "}"
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = Describe(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	default:
		return fmt.Sprint(x)
	}
}
