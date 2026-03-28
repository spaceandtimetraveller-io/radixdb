// Package bytecmp provides byte-oriented prefix comparisons optimized with
// word-at-a-time (and on amd64, assembly) matching.
package bytecmp

// LongestCommonPrefix returns the length of the longest shared prefix of a and b.
func LongestCommonPrefix(a, b []byte) int {
	na, nb := len(a), len(b)
	if na == 0 || nb == 0 {
		return 0
	}
	if na > nb {
		a, b = b, a
	}
	return matchLen(a, b)
}

// HasPrefixBytes reports whether s begins with prefix (same semantics as bytes.HasPrefix).
func HasPrefixBytes(s, prefix []byte) bool {
	lp := len(prefix)
	if lp == 0 {
		return true
	}
	if len(s) < lp {
		return false
	}
	// Shorter slice first: matchLen scans at most len(prefix) bytes (no LongestCommonPrefix swap).
	return matchLen(prefix, s) == lp
}
