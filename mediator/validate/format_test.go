package validate

import (
	"strings"
	"testing"
)

func TestFormats(t *testing.T) {
	maxLabel := strings.Repeat("a", 63)
	longLabel := strings.Repeat("a", 64)
	longHost := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63)
	// maxHost is 253 characters; overHost adds one to its last label, which
	// stays a valid label, so only the total length rejects it.
	maxHost := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	overHost := maxHost + "e"
	// maxEmail is 254 characters with a 64-character local part; overEmail
	// adds one to the last domain label, again valid on its own.
	maxLocal := strings.Repeat("a", 64)
	maxEmail := maxLocal + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	overEmail := maxEmail + "e"
	tests := []struct {
		name  string
		check func(string) bool
		ok    []string
		bad   []string
	}{
		{"uuid", isUUID,
			[]string{"123e4567-e89b-12d3-a456-426614174000", "00000000-0000-0000-0000-000000000000", "A1B2C3D4-E5F6-7890-ABCD-EF1234567890"},
			[]string{"", "123e4567e89b12d3a456426614174000", "123e4567-e89b-12d3-a456-42661417400g", "{123e4567-e89b-12d3-a456-426614174000}"}},
		{"datetime", isDateTime,
			[]string{"2024-01-02T03:04:05Z", "2024-01-02T03:04:05.123+02:00", "2024-12-31T23:59:59-08:00"},
			[]string{"", "2024-01-02", "2024-01-02 03:04:05Z", "2024-13-01T00:00:00Z", "2024-02-30T00:00:00Z"}},
		{"ipv4", isIPv4,
			[]string{"0.0.0.0", "1.2.3.4", "255.255.255.255", "10.0.0.1", "9.9.9.9"},
			[]string{"", "1.2.3", "1.2.3.4.5", "01.2.3.4", "256.1.1.1", "a.b.c.d", "1.2.3.", "1234.1.1.1", "1.2.3.+4", "::1"}},
		{"ipv6", isIPv6,
			[]string{"::1", "::", "2001:db8::1", "::ffff:1.2.3.4", "fe80::1"},
			[]string{"", "1.2.3.4", "fe80::1%eth0", "::g", "1:2:3:4:5:6:7:8:9"}},
		{"hostname", isHostname,
			[]string{"localhost", "example.com", "example.com.", "a-b.c-d.io", "123", "x" + strings.Repeat(".x", 100),
				"az.AZ.09", maxLabel + ".com", maxHost, maxHost + "."},
			[]string{"", ".", "-a.com", "a-.com", "a_b.com", "a..b", "a b", longLabel + ".com", overHost, longHost, "ex@mple.com"}},
		{"email", isEmail,
			[]string{"a@b.com", "first.last+tag@example.co.uk", `"quoted local"@example.com`, "a@[1.2.3.4]", "a@[IPv6:::1]", "x@localhost", "a!#$%&'*+-/=?^_`{|}~b@example.com",
				"AZaz09@example.com", maxLocal + "@example.com", maxEmail},
			[]string{"", "noat", "@example.com", ".a@example.com", "a.@example.com", "a..b@example.com", "a b@example.com", "a@-x.com",
				"a@[999.1.1.1]", "a@[IPv6:::g]", `"a\"b"@example.com`, `"@example.com`, "a@b@example.com", strings.Repeat("a", 65) + "@example.com", overEmail, strings.Repeat("a", 250) + "@example.com", "a@"}},
		{"url", isURL,
			[]string{"https://example.com/x?y=1#z", "mailto:a@b.com", "http://[::1]:8080/", "ftp://user:pw@host/path", "urn:isbn:123"},
			[]string{"", "/relative/path", "example.com", "://missing-scheme", "http://[fe80::1%25eth0]/", "http://exa mple.com"}},
	}
	for _, tc := range tests {
		for _, s := range tc.ok {
			if !tc.check(s) {
				t.Errorf("%s(%q) = false, want true", tc.name, s)
			}
		}
		for _, s := range tc.bad {
			if tc.check(s) {
				t.Errorf("%s(%q) = true, want false", tc.name, s)
			}
		}
	}
}

func TestFormatTableCoversRules(t *testing.T) {
	for _, k := range []ruleKind{ruleEmail, ruleUUID, ruleURL, ruleDateTime, ruleIPv4, ruleIPv6, ruleHostname} {
		f, ok := formats[k]
		if !ok || f.schema == "" || f.check == nil || f.msg == "" {
			t.Errorf("format %s is incomplete: %+v", ruleSpecs[k].name, f)
		}
	}
}
