// Package radixdb implements a mutable mmap-backed radix tree with copy-on-write inserts.
//
// Reads use atomic.Load on the root offset (no reader mutex). Writes are serialized with a mutex.
//
// Duplicate keys store rows in a prepend-linked leaf chain (O(1) bytes per append vs re-encoding the full leaf).
package radixdb
