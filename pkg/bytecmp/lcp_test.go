package bytecmp

import (
	"bytes"
	"testing"
)

func TestLongestCommonPrefix(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "abc", 0},
		{"abc", "abc", 3},
		{"abcd", "abce", 3},
		{"ab", "abc", 2},
		{"x", "y", 0},
	}
	for _, tc := range cases {
		got := LongestCommonPrefix([]byte(tc.a), []byte(tc.b))
		if got != tc.want {
			t.Errorf("LCP(%q,%q)=%d want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestHasPrefixBytes(t *testing.T) {
	for i := 0; i < 200; i++ {
		a := bytes.Repeat([]byte("x"), i)
		for j := 0; j <= i; j++ {
			p := a[:j]
			if !HasPrefixBytes(a, p) {
				t.Fatalf("i=%d j=%d", i, j)
			}
			if j < i {
				wrong := append(append([]byte(nil), p...), 0)
				if HasPrefixBytes(a, wrong) {
					t.Fatalf("false positive i=%d j=%d", i, j)
				}
			}
		}
	}
}
