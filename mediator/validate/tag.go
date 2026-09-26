package validate

import (
	"errors"
	"fmt"
	"strings"
)

// ruleKind identifies one rule of the closed tag grammar.
type ruleKind uint8

const (
	ruleNone ruleKind = iota
	ruleRequired
	ruleAllowEmpty
	ruleMin
	ruleMax
	ruleGT
	ruleGTE
	ruleLT
	ruleLTE
	ruleLen
	rulePattern
	ruleOneOf
	ruleEmail
	ruleUUID
	ruleURL
	ruleDateTime
	ruleIPv4
	ruleIPv6
	ruleHostname
	ruleUnique
	ruleDive
)

// ruleSpec is the syntax of one rule: its name and whether it takes a value.
type ruleSpec struct {
	name   string
	hasArg bool
}

// ruleSpecs is the closed grammar, indexed by ruleKind. It is never mutated.
var ruleSpecs = [...]ruleSpec{
	ruleRequired:   {"required", false},
	ruleAllowEmpty: {"allowempty", false},
	ruleMin:        {"min", true},
	ruleMax:        {"max", true},
	ruleGT:         {"gt", true},
	ruleGTE:        {"gte", true},
	ruleLT:         {"lt", true},
	ruleLTE:        {"lte", true},
	ruleLen:        {"len", true},
	rulePattern:    {"pattern", true},
	ruleOneOf:      {"oneof", true},
	ruleEmail:      {"email", false},
	ruleUUID:       {"uuid", false},
	ruleURL:        {"url", false},
	ruleDateTime:   {"datetime", false},
	ruleIPv4:       {"ipv4", false},
	ruleIPv6:       {"ipv6", false},
	ruleHostname:   {"hostname", false},
	ruleUnique:     {"unique", false},
	ruleDive:       {"dive", false},
}

// ruleByName maps rule names to kinds. It is built once and never mutated.
var ruleByName = func() map[string]ruleKind {
	m := make(map[string]ruleKind, len(ruleSpecs))
	for k, s := range ruleSpecs {
		if s.name != "" {
			m[s.name] = ruleKind(k)
		}
	}
	return m
}()

// rule is one parsed rule: its kind and the raw argument text (empty for
// rules that take no value).
type rule struct {
	kind ruleKind
	arg  string
}

func (r rule) name() string { return ruleSpecs[r.kind].name }

// parseTag parses a validate tag into rules. It is purely syntactic: it
// checks names against the closed grammar, that valued rules have a value and
// flag rules have none, that no rule repeats within a dive segment, and that
// pattern is last. Type-directed checks (numbers, supported kinds) happen in
// the compiler. An empty or blank tag yields no rules.
func parseTag(tag string) ([]rule, error) {
	if strings.TrimSpace(tag) == "" {
		return nil, nil
	}
	var (
		rules []rule
		errs  []error
		seen  = make(map[ruleKind]bool)
	)
	rest := tag
	for {
		// pattern swallows everything up to the end of the tag, so it is
		// recognized before the split on commas.
		if head, tail, ok := strings.Cut(rest, "="); ok && strings.TrimSpace(head) == "pattern" {
			switch seg := trailingRule(tail); {
			case tail == "":
				errs = append(errs, errors.New(`rule "pattern" requires a value`))
			case seg != "":
				errs = append(errs, fmt.Errorf(`rule "pattern" must be the last rule: its value runs to the end of the tag but %q looks like a rule`, seg))
			default:
				rules = append(rules, rule{kind: rulePattern, arg: tail})
			}
			break
		}
		tok, after, more := strings.Cut(rest, ",")
		name, arg, hasArg := strings.Cut(tok, "=")
		name, arg = strings.TrimSpace(name), strings.TrimSpace(arg)
		kind, known := ruleByName[name]
		switch {
		case name == "":
			errs = append(errs, errors.New("empty rule"))
		case !known:
			errs = append(errs, fmt.Errorf("unknown rule %q", name))
		case ruleSpecs[kind].hasArg && arg == "":
			errs = append(errs, fmt.Errorf("rule %q requires a value", name))
		case !ruleSpecs[kind].hasArg && hasArg:
			errs = append(errs, fmt.Errorf("rule %q takes no value", name))
		case kind == ruleDive:
			rules = append(rules, rule{kind: ruleDive})
			clear(seen)
		case seen[kind]:
			errs = append(errs, fmt.Errorf("duplicate rule %q", name))
		default:
			seen[kind] = true
			rules = append(rules, rule{kind: kind, arg: arg})
		}
		if !more {
			break
		}
		rest = after
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return rules, nil
}

// trailingRule reports the first comma-separated segment of a pattern value
// (after the first) whose name is a known rule, which almost certainly means
// the author put pattern before another rule. It returns "" when none looks
// like a rule.
func trailingRule(pattern string) string {
	segs := strings.Split(pattern, ",")
	for _, seg := range segs[1:] {
		name, _, _ := strings.Cut(seg, "=")
		if _, ok := ruleByName[strings.TrimSpace(name)]; ok {
			return seg
		}
	}
	return ""
}

// printTag renders rules back into tag syntax. parseTag(printTag(r)) yields r
// for any r produced by parseTag.
func printTag(rules []rule) string {
	var b strings.Builder
	for i, r := range rules {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(r.name())
		if ruleSpecs[r.kind].hasArg {
			b.WriteByte('=')
			b.WriteString(r.arg)
		}
	}
	return b.String()
}

// splitDive separates the rules that apply to a container from the rules
// after the first dive, which apply to its elements.
func splitDive(rules []rule) (own, rest []rule, dived bool) {
	for i, r := range rules {
		if r.kind == ruleDive {
			return rules[:i], rules[i+1:], true
		}
	}
	return rules, nil, false
}

// hasRule reports whether rules contains a rule of the given kind.
func hasRule(rules []rule, kind ruleKind) bool {
	for _, r := range rules {
		if r.kind == kind {
			return true
		}
	}
	return false
}
