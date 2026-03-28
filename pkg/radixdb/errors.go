package radixdb

import "errors"

var (
	// errWalkStop is returned internally when a walk callback requests stop; WalkPrefixBytes maps it to nil.
	errWalkStop = errors.New("radixdb: walk stopped by callback")

	ErrMagic     = errors.New("radixdb: bad magic")
	ErrVersion   = errors.New("radixdb: unsupported version")
	ErrCorrupt   = errors.New("radixdb: corrupt node or leaf")
	ErrReadOnly  = errors.New("radixdb: read-only")
	ErrKeyTooBig = errors.New("radixdb: key too big")
	ErrNoPath    = errors.New("radixdb: database path not set (open with Open to enable compaction)")
)
