//go:build amd64 && !appengine && !noasm && gc && !purego

// Assembly matchLen derived from github.com/klauspost/compress zstd (Apache-2.0).

package bytecmp

// matchLen returns how many leading bytes of a match b.
// Precondition: len(a) <= len(b).
//
//go:noescape
func matchLen(a, b []byte) int
