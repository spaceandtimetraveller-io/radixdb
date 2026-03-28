package radixdb

// LiveBytes estimates the number of bytes on disk reachable from the current root
// (aligned node encodings and leaf chunks). Compare to FileSizeBytes for reclaim heuristics.
// When the tree is empty (root == 0), returns full file size so waste is reported as zero.
func (db *DB) LiveBytes() (uint64, error) {
	db.readCloseMu.RLock()
	defer db.readCloseMu.RUnlock()
	return db.liveBytesUnlocked()
}

func (db *DB) liveBytesUnlocked() (uint64, error) {
	nb := db.nBlocks.Load()
	fileBytes := uint64(nb) * uint64(BlockSize)
	for spin := 0; spin < 1024; spin++ {
		root := Ref(db.root.Load())
		if root == 0 {
			return fileBytes, nil
		}
		if root.Block() >= nb {
			continue
		}
		sum, err := reachableBytesFromRoot(db.bm, root, nb)
		if err != nil {
			return 0, err
		}
		return sum, nil
	}
	return 0, ErrCorrupt
}

func reachableBytesFromRoot(bm *blockMgr, root Ref, nBlocks uint32) (uint64, error) {
	var sum uint64
	seen := make(map[Ref]struct{})
	stack := []Ref{root}
	for len(stack) > 0 {
		r := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if r == 0 {
			continue
		}
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		if r.Block() >= nBlocks {
			return 0, ErrCorrupt
		}
		blk, err := bm.readBlock(r.Block())
		if err != nil {
			return 0, err
		}
		off := int(r.Offset())
		var n decodedNode
		if err := decodeNodeInto(&n, blk, off); err != nil {
			return 0, err
		}
		enc := encodeNode(&n)
		sum += uint64(align8(len(enc)))

		for lr := n.leaf; lr != 0; {
			if lr.Block() >= nBlocks {
				return 0, ErrCorrupt
			}
			lb, err := bm.readBlock(lr.Block())
			if err != nil {
				return 0, err
			}
			lo := int(lr.Offset())
			row, next, err := decodeChunkAt(lb, lo)
			if err != nil {
				return 0, err
			}
			pl := len(row.FullPath)
			sum += uint64(align8(12 + pl + 8))
			lr = next
		}

		for _, e := range n.edges {
			if e.child != 0 {
				stack = append(stack, e.child)
			}
		}
	}
	return sum, nil
}
