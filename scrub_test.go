package pagerless

import "testing"

func TestScrub(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"empty", "", ""},
		{"plain text untouched", "order 42 failed after 3 retries", "order 42 failed after 3 retries"},
		{"url credentials", "dial postgres://app:hunter2@db.internal:5432/shop failed", "dial postgres://[redacted]@db.internal:5432/shop failed"},
		{"jwt", "token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U expired", "token [jwt] expired"},
		{"bearer header", "Authorization: Bearer abcdef0123456789xyz", "Authorization: Bearer [redacted]"},
		{"stripe key", "stripe: invalid key sk_live_51HxQabcdEFGHijkl", "stripe: invalid key [token]"},
		{"pagerless key", "using pl_live_8f2cabc123a91", "using [token]"},
		{"github token", "ghp_abcdefghijklmnopqrstuvwxyz0123456789", "[token]"},
		{"aws key", "AKIAIOSFODNN7EXAMPLE rejected", "[token] rejected"},
		{"password pair", "login password=hunter2 user=bob", "login password=[redacted] user=bob"},
		{"json secret", `{"api_key": "abc123", "n": 1}`, `{"api_key": "[redacted]", "n": 1}`},
		{"email", "no account for maya@example.co.uk", "no account for [email]"},
		{"card spaced", "charge 4242 4242 4242 4242 declined", "charge [card] declined"},
		{"card dashed", "card 5555-5555-5555-4444", "card [card]"},
		{"card plain", "pan=4111111111111111", "pan=[card]"},
		{"non-luhn digits kept", "order 1234567812345678 not found", "order 1234567812345678 not found"},
		{"intl phone", "sms to +14155550132 failed", "sms to [phone] failed"},
		{"intl phone spaced", "call +44 20 7946 0958", "call [phone]"},
		{"us phone", "call (415) 555-0132 now", "call [phone] now"},
		{"dashed phone", "call 415-555-0132", "call [phone]"},
		{"date kept", "on 2026-10-22 at 12:00:01", "on 2026-10-22 at 12:00:01"},
		{"ip kept", "connect 10.0.0.12:5432 refused", "connect 10.0.0.12:5432 refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Scrub(tc.in); got != tc.want {
				t.Errorf("Scrub(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestLuhn(t *testing.T) {
	for s, want := range map[string]bool{
		"4242424242424242":    true,
		"4242 4242 4242 4241": false,
		"378282246310005":     true,
		"123":                 false,
	} {
		if luhn(s) != want {
			t.Errorf("luhn(%q) != %v", s, want)
		}
	}
}

func TestCleanTagsRedactsSecretKeysAndBounds(t *testing.T) {
	in := map[string]string{"plan": "pro", "session_id": "abc", "user": "maya@example.com"}
	got := cleanTags(in)
	if got["plan"] != "pro" || got["session_id"] != redacted || got["user"] != "[email]" {
		t.Fatalf("got %v", got)
	}
	big := map[string]string{}
	for i := range 80 {
		big[string(rune('a'+i%26))+string(rune('A'+i/26))] = "v"
	}
	if n := len(cleanTags(big)); n != maxTags {
		t.Fatalf("tags not bounded: %d", n)
	}
}

func TestEventIsScrubbedBeforeQueueing(t *testing.T) {
	c, _ := testClient(t, "http://unused")
	c.Capture(errString("refund to maya@example.com with card 4242424242424242 failed"))
	ev := c.pending()[0]
	if ev.Error.Message != "refund to [email] with card [card] failed" {
		t.Fatalf("message not scrubbed: %q", ev.Error.Message)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
