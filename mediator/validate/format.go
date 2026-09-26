package validate

import (
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// uuidRE matches the canonical 8-4-4-4-12 hexadecimal UUID form, which is
// what the JSON Schema "uuid" format accepts.
var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// formatSpec ties a format rule to its JSON Schema format name, its check,
// and its error message.
type formatSpec struct {
	schema string
	check  func(string) bool
	msg    string
}

// formats is indexed by ruleKind and is never mutated.
var formats = map[ruleKind]formatSpec{
	ruleEmail:    {"email", isEmail, "must be an email address"},
	ruleUUID:     {"uuid", isUUID, "must be a UUID"},
	ruleURL:      {"uri", isURL, "must be a URL"},
	ruleDateTime: {"date-time", isDateTime, "must be an RFC 3339 date-time"},
	ruleIPv4:     {"ipv4", isIPv4, "must be an IPv4 address"},
	ruleIPv6:     {"ipv6", isIPv6, "must be an IPv6 address"},
	ruleHostname: {"hostname", isHostname, "must be a hostname"},
}

func isUUID(s string) bool { return uuidRE.MatchString(s) }

func isDateTime(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

// isIPv4 accepts dotted decimal notation without leading zeros.
func isIPv4(s string) bool {
	groups := strings.Split(s, ".")
	if len(groups) != 4 {
		return false
	}
	for _, g := range groups {
		if g == "" || len(g) > 3 || (len(g) > 1 && g[0] == '0') {
			return false
		}
		n := 0
		for i := 0; i < len(g); i++ {
			if g[i] < '0' || g[i] > '9' {
				return false
			}
			n = n*10 + int(g[i]-'0')
		}
		if n > 255 {
			return false
		}
	}
	return true
}

// isIPv6 accepts textual IPv6 addresses without a zone.
func isIPv6(s string) bool {
	if !strings.Contains(s, ":") {
		return false
	}
	addr, err := netip.ParseAddr(s)
	return err == nil && addr.Zone() == ""
}

// isHostname implements RFC 1123 host names: dot separated labels of 1 to 63
// letters, digits, and hyphens, not starting or ending with a hyphen, at most
// 253 characters in total; one trailing dot is tolerated.
func isHostname(s string) bool {
	s = strings.TrimSuffix(s, ".")
	if len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if len(label) < 1 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			if !isAlnumOrHyphen(label[i]) {
				return false
			}
		}
	}
	return true
}

func isAlnumOrHyphen(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-'
}

// isEmail accepts local@domain where the local part is a dot-atom or a
// quoted string of at most 64 characters and the domain is a host name or a
// bracketed IP literal; the whole address is at most 254 characters.
func isEmail(s string) bool {
	if len(s) > 254 {
		return false
	}
	at := strings.LastIndexByte(s, '@')
	if at < 0 {
		return false
	}
	local, domain := s[:at], s[at+1:]
	if len(local) == 0 || len(local) > 64 {
		return false
	}
	if len(local) > 1 && local[0] == '"' && local[len(local)-1] == '"' {
		if strings.ContainsAny(local[1:len(local)-1], `\"`) {
			return false
		}
	} else {
		if local[0] == '.' || local[len(local)-1] == '.' || strings.Contains(local, "..") {
			return false
		}
		for _, ch := range local {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune(".!#$%&'*+-/=?^_`{|}~", ch)) {
				return false
			}
		}
	}
	if len(domain) > 1 && domain[0] == '[' && domain[len(domain)-1] == ']' {
		lit := domain[1 : len(domain)-1]
		if v6, ok := strings.CutPrefix(lit, "IPv6:"); ok {
			return isIPv6(v6)
		}
		return isIPv4(lit)
	}
	return isHostname(domain)
}

// isURL accepts absolute URIs: a scheme is mandatory and an IPv6 host must be
// a valid bracketed address.
func isURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil || u.Scheme == "" {
		return false
	}
	if h := u.Hostname(); strings.Contains(h, ":") && !isIPv6(h) {
		return false
	}
	return true
}
