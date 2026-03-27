//go:build !amd64 || appengine || noasm || !gc || purego

// Generic matchLen derived from github.com/klauspost/compress zstd (Apache-2.0, Klaus Post).

package bytecmp

import (
	"encoding/binary"
	"math/bits"
)

// matchLen returns how many leading bytes of a match b.
// Precondition: len(a) <= len(b).
func matchLen(a, b []byte) (n int) {
	left := len(a)
	for left >= 8 {
		diff := binary.LittleEndian.Uint64(a[n:n+8]) ^ binary.LittleEndian.Uint64(b[n:n+8])
		if diff != 0 {
			return n + bits.TrailingZeros64(diff)>>3
		}
		n += 8
		left -= 8
	}
	a = a[n:]
	b = b[n:]

	for i := range a {
		if a[i] != b[i] {
			break
		}
		n++
	}
	return n
}
