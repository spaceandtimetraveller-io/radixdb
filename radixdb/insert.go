package radixdb

import "artbenchmark/radixdb/bytecmp"

// insert follows github.com/hashicorp/go-immutable-radix Txn.insert for structure;
// leaf values are multi-row blobs merged on duplicate keys.
// newDistinct is true when this insert is the first row for the key k.
func (db *DB) insert(off uint64, k, search []byte, r Row) (uint64, bool, error) {
	n, err := db.loadNode(off)
	if err != nil {
		return 0, false, err
	}
	nc := cloneDecoded(n)

	if len(search) == 0 {
		var leafOff uint64
		newDistinct := false
		if nc.leaf != 0 {
			leafOff = db.prependLeafChunk(nc.leaf, r)
		} else {
			leafOff = db.appendBytes(encodeLeafSingle(r))
			newDistinct = true
		}
		nc.leaf = leafOff
		return db.writeNode(nc), newDistinct, nil
	}

	label := search[0]
	_, _, childOff := nc.getEdge(label)
	if childOff == 0 {
		leafOff := db.appendBytes(encodeLeafSingle(r))
		child := &decodedNode{
			prefix: append([]byte(nil), search...),
			leaf:   leafOff,
		}
		childNodeOff := db.writeNode(child)
		nc.addEdge(label, childNodeOff)
		return db.writeNode(nc), true, nil
	}

	child, err := db.loadNode(childOff)
	if err != nil {
		return 0, false, err
	}
	commonPrefix := bytecmp.LongestCommonPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		search = search[commonPrefix:]
		newChildOff, subDistinct, err := db.insert(childOff, k, search, r)
		if err != nil {
			return 0, false, err
		}
		nc.replaceEdge(label, newChildOff)
		return db.writeNode(nc), subDistinct, nil
	}

	// Split the node (search diverges from child.prefix)
	modChild := cloneDecoded(child)
	edgeLabel := modChild.prefix[commonPrefix]
	modChild.prefix = append([]byte(nil), modChild.prefix[commonPrefix:]...)

	splitNode := &decodedNode{
		prefix: append([]byte(nil), search[:commonPrefix]...),
	}
	modChildOff := db.writeNode(modChild)
	splitNode.addEdge(edgeLabel, modChildOff)

	leafOff := db.appendBytes(encodeLeafSingle(r))

	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leafOff
	} else {
		newN := &decodedNode{
			prefix: append([]byte(nil), search...),
			leaf:   leafOff,
		}
		newNOff := db.writeNode(newN)
		splitNode.addEdge(search[0], newNOff)
	}
	splitOff := db.writeNode(splitNode)
	nc.replaceEdge(label, splitOff)
	return db.writeNode(nc), true, nil
}
