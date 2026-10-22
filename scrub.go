package pagerless

import (
	"regexp"
	"strings"
)

// Scrubbing runs on every string that leaves the process (error messages, stacks, titles, tag
// values, routes). It errs on the side of over-redaction: a lost digit in an error message is
// cheap, a leaked card number or token is not.

const redacted = "[redacted]"

var (
	// scheme://user:pass@host
	urlCreds = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@`)
	// JSON Web Tokens: three base64url segments, header starts with {" → eyJ.
	jwtRe = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*`)
	// Authorization: Bearer xyz / Basic xyz
	bearerRe = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`)
	// Well-known vendor token shapes (Stripe, Pagerless, GitHub, Slack, AWS, Google, OpenAI/Anthropic).
	vendorToken = regexp.MustCompile(`\b(?:(?:sk|rk|pk|whsec)_(?:live|test)_[A-Za-z0-9]{8,}|pl_(?:live|test)_[A-Za-z0-9_]{6,}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}|sk-(?:ant-)?[A-Za-z0-9_-]{20,})`)
	// key=value / "key": "value" pairs whose key names a secret.
	secretPair = regexp.MustCompile(`(?i)((?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret|session|cookie|authorization)["']?\s*[:=]\s*["']?)[^\s"'&,;}]+`)
	// Keys whose whole value is a secret (tags).
	secretKey = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|authorization|cookie|session|credential)`)
	emailRe   = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	// 13 to 19 digits, optionally grouped by single spaces or dashes. Confirmed with Luhn.
	cardRe = regexp.MustCompile(`\b\d(?:[ -]?\d){12,18}\b`)
	// Phone numbers: international (+ and 8-15 digits, separators allowed) or grouped national
	// forms like (415) 555-0132, 415-555-0132, 020 7946 0958.
	phoneRe = regexp.MustCompile(`\+\d(?:[\s.-]?\d){7,14}\b|\(\d{2,4}\)\s?\d{3,4}[\s.-]\d{3,4}\b|\b\d{3,4}[\s.-]\d{3,4}[\s.-]\d{4}\b`)
)

// Scrub removes secrets and personal data from s: URL credentials, JWTs, bearer tokens, vendor
// API keys, secret key=value pairs, emails, card numbers (Luhn-checked) and phone numbers.
func Scrub(s string) string {
	if s == "" {
		return s
	}
	s = urlCreds.ReplaceAllString(s, "${1}"+redacted+"@")
	s = jwtRe.ReplaceAllString(s, "[jwt]")
	s = bearerRe.ReplaceAllString(s, "${1} "+redacted)
	s = vendorToken.ReplaceAllString(s, "[token]")
	s = secretPair.ReplaceAllStringFunc(s, func(m string) string {
		sub := secretPair.FindStringSubmatch(m)
		val := m[len(sub[1]):]
		// Leave values an earlier rule already handled ("Authorization: Bearer [redacted]").
		if strings.HasPrefix(val, "[") || strings.EqualFold(val, "bearer") || strings.EqualFold(val, "basic") {
			return m
		}
		return sub[1] + redacted
	})
	s = emailRe.ReplaceAllString(s, "[email]")
	s = cardRe.ReplaceAllStringFunc(s, func(m string) string {
		if luhn(m) {
			return "[card]"
		}
		return m
	})
	s = phoneRe.ReplaceAllString(s, "[phone]")
	return s
}

// luhn reports whether the digits in s (ignoring separators) pass the Luhn checksum.
func luhn(s string) bool {
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	if len(digits) < 13 || len(digits) > 19 {
		return false
	}
	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}
