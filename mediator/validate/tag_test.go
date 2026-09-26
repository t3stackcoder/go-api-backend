package validate

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseTag(t *testing.T) {
	tests := []struct {
		in   string
		want []rule
	}{
		{"", nil},
		{"   ", nil},
		{"required", []rule{{ruleRequired, ""}}},
		{"required,min=1,max=10", []rule{{ruleRequired, ""}, {ruleMin, "1"}, {ruleMax, "10"}}},
		{" required , min= 1 ,max =2 ", []rule{{ruleRequired, ""}, {ruleMin, "1"}, {ruleMax, "2"}}},
		{"pattern=^[a-z]{3,32}$", []rule{{rulePattern, "^[a-z]{3,32}$"}}},
		{"required,pattern=^a,b=c$", []rule{{ruleRequired, ""}, {rulePattern, "^a,b=c$"}}},
		{"pattern = x ", []rule{{rulePattern, " x "}}},
		{"oneof=a b  c", []rule{{ruleOneOf, "a b  c"}}},
		{"oneof=a=b", []rule{{ruleOneOf, "a=b"}}},
		{"dive,dive,min=1", []rule{{ruleDive, ""}, {ruleDive, ""}, {ruleMin, "1"}}},
		{"min=1,dive,min=2", []rule{{ruleMin, "1"}, {ruleDive, ""}, {ruleMin, "2"}}},
		{"email,uuid,url,datetime,ipv4,ipv6,hostname,unique,allowempty,len=3,gt=1,gte=2,lt=3,lte=4",
			[]rule{{ruleEmail, ""}, {ruleUUID, ""}, {ruleURL, ""}, {ruleDateTime, ""}, {ruleIPv4, ""}, {ruleIPv6, ""},
				{ruleHostname, ""}, {ruleUnique, ""}, {ruleAllowEmpty, ""}, {ruleLen, "3"}, {ruleGT, "1"}, {ruleGTE, "2"}, {ruleLT, "3"}, {ruleLTE, "4"}}},
	}
	for _, tc := range tests {
		got, err := parseTag(tc.in)
		if err != nil {
			t.Errorf("parseTag(%q): unexpected error %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseTag(%q) = %v, want %v", tc.in, got, tc.want)
		}
		// Round trip through print.
		again, err := parseTag(printTag(got))
		if err != nil || !reflect.DeepEqual(again, got) {
			t.Errorf("round trip of %q via %q gave %v, %v", tc.in, printTag(got), again, err)
		}
	}
}

func TestParseTagErrors(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"foo", []string{`unknown rule "foo"`}},
		{"required,", []string{"empty rule"}},
		{",required", []string{"empty rule"}},
		{",", []string{"empty rule"}},
		{"min", []string{`rule "min" requires a value`}},
		{"min=", []string{`rule "min" requires a value`}},
		{"required=1", []string{`rule "required" takes no value`}},
		{"required,required", []string{`duplicate rule "required"`}},
		{"pattern=", []string{`rule "pattern" requires a value`}},
		{"pattern", []string{`rule "pattern" requires a value`}},
		{"pattern=^a$,min=1", []string{`rule "pattern" must be the last rule`}},
		{"pattern=x,pattern=y", []string{`rule "pattern" must be the last rule`}},
		{"pattern=^a$, required", []string{`rule "pattern" must be the last rule`}},
		{"foo,bar,min", []string{`unknown rule "foo"`, `unknown rule "bar"`, `rule "min" requires a value`}},
	}
	for _, tc := range tests {
		rules, err := parseTag(tc.in)
		if err == nil {
			t.Errorf("parseTag(%q) = %v, want error", tc.in, rules)
			continue
		}
		for _, w := range tc.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("parseTag(%q) error %q does not contain %q", tc.in, err, w)
			}
		}
	}
}

func TestPrintTag(t *testing.T) {
	got := printTag([]rule{{ruleRequired, ""}, {ruleMin, "1"}, {ruleDive, ""}, {rulePattern, "^a,b$"}})
	if want := "required,min=1,dive,pattern=^a,b$"; got != want {
		t.Fatalf("printTag = %q, want %q", got, want)
	}
	if printTag(nil) != "" {
		t.Fatalf("printTag(nil) = %q", printTag(nil))
	}
}

func TestSplitDive(t *testing.T) {
	own, rest, dived := splitDive([]rule{{ruleMin, "1"}, {ruleDive, ""}, {ruleMax, "2"}})
	if !dived || len(own) != 1 || len(rest) != 1 || own[0].kind != ruleMin || rest[0].kind != ruleMax {
		t.Fatalf("splitDive = %v %v %v", own, rest, dived)
	}
	own, rest, dived = splitDive([]rule{{ruleMin, "1"}})
	if dived || len(own) != 1 || rest != nil {
		t.Fatalf("splitDive without dive = %v %v %v", own, rest, dived)
	}
	if hasRule(own, ruleDive) || !hasRule(own, ruleMin) {
		t.Fatal("hasRule is wrong")
	}
}

// FuzzTagGrammar checks that arbitrary tag strings never panic the parser
// and that parse -> print -> parse is the identity on accepted tags.
func FuzzTagGrammar(f *testing.F) {
	for _, s := range []string{
		"", "required", "required,min=1,max=10", "pattern=^[A-Z0-9-]{3,32}$",
		"oneof=a b c", "min=1,dive,min=2", "dive,dive,unique", "min=,max", "pattern=^a$,min=1",
		" required , len = 3 ", "oneof=a=b,pattern=x=y,z", "email,uuid,url,datetime,ipv4,ipv6,hostname",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		rules, err := parseTag(s)
		if err != nil {
			if rules != nil {
				t.Fatalf("parseTag(%q) returned rules with an error: %v", s, err)
			}
			return
		}
		printed := printTag(rules)
		again, err := parseTag(printed)
		if err != nil {
			t.Fatalf("parseTag(%q) accepted but its print %q is rejected: %v", s, printed, err)
		}
		if !reflect.DeepEqual(rules, again) {
			t.Fatalf("parseTag(%q) = %v but reparse of %q = %v", s, rules, printed, again)
		}
		if printTag(again) != printed {
			t.Fatalf("print is not stable: %q vs %q", printed, printTag(again))
		}
	})
}
