// Package radixdb is a block-paged radix tree index (on-disk magic RDX2) inspired by DuckDB-style storage:
// fixed BlockSize pages, Ref = (blockID<<12)|offset, buffer manager with per-block cache,
// append-only copy-on-write inserts and atomic root publication.
//
// It provides Row, Insert, Get, WalkPrefix, Stats, and related helpers over the RDX2 file format.
//
// Compaction: dead space is estimated as file size minus a DFS sum of reachable node/leaf allocations
// (see LiveBytes). ShouldCompact / CompactIfNeeded apply size, reclaim, cooldown, and waste-ratio
// thresholds (configurable; persisted in the v2 header). After at least one successful CompactIfNeeded,
// further runs skip the expensive checks when no Insert has occurred since (no_writes_since_compact).
// CompactFile and CompactIfNeeded rewrite the live tree to a new file; replacement uses a .compact.bak
// rename sequence when replacing an existing path.
package radixdb
