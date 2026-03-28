package radixdb

import "artbenchmark/pkg/bytecmp"

func (db *DB) prependLeafChunk(oldHead Ref, r Row) (Ref, error) {
	chunk := encodeChunk(r, oldHead)
	return db.allocRaw(chunk)
}

// insert returns new root Ref and whether this insert added a new distinct key.
func (db *DB) insert(off Ref, k, search []byte, r Row) (Ref, bool, error) {
	n, err := db.loadNodeRef(off)
	if err != nil {
		return 0, false, err
	}
	nc := cloneDecoded(n)

	if len(search) == 0 {
		var leafRef Ref
		newDistinct := false
		if nc.leaf != 0 {
			leafRef, err = db.prependLeafChunk(nc.leaf, r)
			if err != nil {
				return 0, false, err
			}
		} else {
			b := encodeLeafSingle(r)
			leafRef, err = db.allocRaw(b)
			if err != nil {
				return 0, false, err
			}
			newDistinct = true
		}
		nc.leaf = leafRef
		newRef, werr := db.writeNode(nc)
		return newRef, newDistinct, werr
	}

	label := search[0]
	_, _, childOff := nc.getEdge(label)
	if childOff == 0 {
		b := encodeLeafSingle(r)
		leafRef, err := db.allocRaw(b)
		if err != nil {
			return 0, false, err
		}
		child := &decodedNode{
			prefix: append([]byte(nil), search...),
			leaf:   leafRef,
		}
		childRef, err := db.writeNode(child)
		if err != nil {
			return 0, false, err
		}
		nc.addEdge(label, childRef)
		newRef, werr := db.writeNode(nc)
		return newRef, true, werr
	}

	child, err := db.loadNodeRef(childOff)
	if err != nil {
		return 0, false, err
	}
	commonPrefix := bytecmp.LongestCommonPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		search = search[commonPrefix:]
		newChildRef, subDistinct, err := db.insert(childOff, k, search, r)
		if err != nil {
			return 0, false, err
		}
		nc.replaceEdge(label, newChildRef)
		newRef, werr := db.writeNode(nc)
		return newRef, subDistinct, werr
	}

	modChild := cloneDecoded(child)
	edgeLabel := modChild.prefix[commonPrefix]
	modChild.prefix = append([]byte(nil), modChild.prefix[commonPrefix:]...)

	splitNode := &decodedNode{
		prefix: append([]byte(nil), search[:commonPrefix]...),
	}
	modChildRef, err := db.writeNode(modChild)
	if err != nil {
		return 0, false, err
	}
	splitNode.addEdge(edgeLabel, modChildRef)

	b := encodeLeafSingle(r)
	leafRef, err := db.allocRaw(b)
	if err != nil {
		return 0, false, err
	}

	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leafRef
	} else {
		newN := &decodedNode{
			prefix: append([]byte(nil), search...),
			leaf:   leafRef,
		}
		newNRef, err := db.writeNode(newN)
		if err != nil {
			return 0, false, err
		}
		splitNode.addEdge(search[0], newNRef)
	}
	splitRef, err := db.writeNode(splitNode)
	if err != nil {
		return 0, false, err
	}
	nc.replaceEdge(label, splitRef)
	newRef, werr := db.writeNode(nc)
	return newRef, true, werr
}
