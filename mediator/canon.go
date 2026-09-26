package mediator

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// CanonicalJSON encodes v as JSON with object keys sorted, no insignificant
// whitespace, and numbers in shortest round-trip form. Two values that encode
// to the same JSON document under any key order or number spelling produce
// identical bytes. Idempotency hashing and cache keys use it.
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonicalizeJSON(raw)
}

// CanonicalHash returns SHA-256 over CanonicalJSON(v).
func CanonicalHash(v any) ([32]byte, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// CanonicalizeJSON rewrites a JSON document in canonical form. Duplicate
// object names and invalid UTF-8 are rejected by the decoder.
func CanonicalizeJSON(raw []byte) ([]byte, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	node, err := readNode(dec)
	if err != nil {
		return nil, err
	}
	// Reject trailing data.
	if _, err := dec.ReadToken(); err != io.EOF {
		if err == nil {
			return nil, errors.New("mediator: canonical json: trailing data")
		}
		return nil, err
	}
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf)
	if err := writeNode(enc, node); err != nil {
		return nil, err // covergate:ignore a bytes.Buffer never fails to write
	}
	out := bytes.TrimRight(buf.Bytes(), "\n")
	return out, nil
}

type jsonNode struct {
	kind   byte // '{', '[', '"', '0', 't', 'f', 'n'
	str    string
	num    string
	obj    []jsonMember
	arr    []*jsonNode
	truthy bool
}

type jsonMember struct {
	key string
	val *jsonNode
}

func readNode(dec *jsontext.Decoder) (*jsonNode, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case '{':
		n := &jsonNode{kind: '{'}
		for {
			if dec.PeekKind() == '}' {
				if _, err := dec.ReadToken(); err != nil {
					return nil, err // covergate:ignore PeekKind saw the token; ReadToken cannot fail
				}
				break
			}
			kt, err := dec.ReadToken()
			if err != nil {
				return nil, err
			}
			key := kt.String() // valid only until the next read
			val, err := readNode(dec)
			if err != nil {
				return nil, err
			}
			n.obj = append(n.obj, jsonMember{key: key, val: val})
		}
		// The decoder has already rejected duplicate names, so a stable sort
		// by key is a total order.
		slices.SortStableFunc(n.obj, func(a, b jsonMember) int { return strings.Compare(a.key, b.key) })
		return n, nil
	case '[':
		n := &jsonNode{kind: '['}
		for {
			if dec.PeekKind() == ']' {
				if _, err := dec.ReadToken(); err != nil {
					return nil, err // covergate:ignore PeekKind saw the token; ReadToken cannot fail
				}
				break
			}
			val, err := readNode(dec)
			if err != nil {
				return nil, err
			}
			n.arr = append(n.arr, val)
		}
		return n, nil
	case '"':
		return &jsonNode{kind: '"', str: tok.String()}, nil
	case '0':
		return &jsonNode{kind: '0', num: canonicalNumber(tok.String())}, nil
	case 't':
		return &jsonNode{kind: 't', truthy: true}, nil
	case 'f':
		return &jsonNode{kind: 'f'}, nil
	case 'n':
		return &jsonNode{kind: 'n'}, nil
	default:
		return nil, fmt.Errorf("mediator: canonical json: unexpected token %v", tok) // covergate:ignore the decoder rejects a stray '}' or ']' before returning a token
	}
}

// canonicalNumber returns one spelling per numeric value. Integer literals are
// kept verbatim (so 64-bit and larger integers lose nothing); anything with a
// fraction or exponent is parsed as float64 and re-encoded: integral values
// below 1e21 as plain digits, so 100.0, 1e2, and 1000000.0 agree with 100 and
// 1000000; everything else in shortest round-trip form, so 1.50, 1.5e0, and
// 1.5 agree.
func canonicalNumber(s string) string {
	if !strings.ContainsAny(s, ".eE") {
		if s == "-0" {
			return "0"
		}
		return s
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return s
	}
	if f == 0 {
		return "0"
	}
	if f == math.Trunc(f) && math.Abs(f) < 1e21 {
		// Shortest digits without an exponent, which is also how json.Marshal
		// spells an integral float64 below 1e21.
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	out := strconv.FormatFloat(f, 'g', -1, 64)
	// FormatFloat may emit "1e+06"; JSON permits it, but normalize to the
	// same form json.Marshal uses for float64 so both paths agree.
	if strings.Contains(out, "e") {
		out = strings.Replace(out, "e+", "e", 1)
	}
	return out
}

func writeNode(enc *jsontext.Encoder, n *jsonNode) error {
	switch n.kind {
	case '{':
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return err
		}
		for _, m := range n.obj {
			if err := enc.WriteToken(jsontext.String(m.key)); err != nil {
				return err
			}
			if err := writeNode(enc, m.val); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndObject)
	case '[':
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, v := range n.arr {
			if err := writeNode(enc, v); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)
	case '"':
		return enc.WriteToken(jsontext.String(n.str))
	case '0':
		return enc.WriteValue(jsontext.Value(n.num))
	case 't':
		return enc.WriteToken(jsontext.True)
	case 'f':
		return enc.WriteToken(jsontext.False)
	default:
		return enc.WriteToken(jsontext.Null)
	}
}
