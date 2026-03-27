package radixdb

import (
	"encoding/binary"
	"sort"
)

// decodedNode mirrors iradix internal node layout (prefix + sorted edges + optional leaf).
type decodedNode struct {
	prefix []byte
	edges  []edge
	leaf   uint64 // offset of leaf blob, 0 if none
}

type edge struct {
	label byte
	off   uint64 // child node offset
}

const nodeHdr = 16 // prefixLen u16, nChild u16, hasLeaf u8, pad, leafOff u64

func nodeSize(prefixLen int, nChild int) int {
	return nodeHdr + prefixLen + nChild*16 // label u8 + pad7 + child u64
}

func encodeNode(n *decodedNode) []byte {
	if len(n.prefix) > 65535 {
		panic("radixdb: prefix too long")
	}
	if len(n.edges) > 65535 {
		panic("radixdb: too many edges")
	}
	pl := uint16(len(n.prefix))
	nc := uint16(len(n.edges))
	var hasLeaf byte
	if n.leaf != 0 {
		hasLeaf = 1
	}
	sz := nodeSize(len(n.prefix), len(n.edges))
	b := make([]byte, sz)
	binary.LittleEndian.PutUint16(b[0:2], pl)
	binary.LittleEndian.PutUint16(b[2:4], nc)
	b[4] = hasLeaf
	b[5] = 0
	binary.LittleEndian.PutUint64(b[8:16], n.leaf)
	copy(b[nodeHdr:], n.prefix)
	off := nodeHdr + len(n.prefix)
	// sort edges by label
	edges := append([]edge(nil), n.edges...)
	sort.Slice(edges, func(i, j int) bool { return edges[i].label < edges[j].label })
	for _, e := range edges {
		b[off] = e.label
		off++
		// 7 bytes pad
		off += 7
		binary.LittleEndian.PutUint64(b[off:off+8], e.off)
		off += 8
	}
	return b
}

func decodeNode(data []byte, at int) (*decodedNode, error) {
	if at < 0 || at+nodeHdr > len(data) {
		return nil, ErrCorrupt
	}
	pl := int(binary.LittleEndian.Uint16(data[at : at+2]))
	nc := int(binary.LittleEndian.Uint16(data[at+2 : at+4]))
	hasLeaf := data[at+4]
	leafOff := binary.LittleEndian.Uint64(data[at+8 : at+16])
	if pl > len(data)-at-nodeHdr {
		return nil, ErrCorrupt
	}
	need := nodeHdr + pl + nc*16
	if at+need > len(data) {
		return nil, ErrCorrupt
	}
	// Prefix bytes are immutable in the mmap; cloneDecoded / append copy when needed.
	prefix := data[at+nodeHdr : at+nodeHdr+pl]
	off := at + nodeHdr + pl
	edges := make([]edge, nc)
	for i := 0; i < nc; i++ {
		edges[i].label = data[off]
		off += 8 // label + 7 pad
		edges[i].off = binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
	}
	n := &decodedNode{prefix: prefix, edges: edges}
	if hasLeaf != 0 {
		n.leaf = leafOff
	}
	return n, nil
}

func (n *decodedNode) getEdge(label byte) (int, *decodedNode, uint64) {
	idx := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if idx < len(n.edges) && n.edges[idx].label == label {
		return idx, nil, n.edges[idx].off
	}
	return -1, nil, 0
}

func (n *decodedNode) replaceEdge(label byte, newOff uint64) {
	idx := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if idx >= len(n.edges) || n.edges[idx].label != label {
		panic("radixdb: replace missing edge")
	}
	n.edges[idx].off = newOff
}

func (n *decodedNode) addEdge(label byte, off uint64) {
	e := edge{label: label, off: off}
	n.edges = append(n.edges, e)
	sort.Slice(n.edges, func(i, j int) bool { return n.edges[i].label < n.edges[j].label })
}

func (n *decodedNode) delEdge(label byte) {
	idx := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if idx >= len(n.edges) || n.edges[idx].label != label {
		return
	}
	copy(n.edges[idx:], n.edges[idx+1:])
	n.edges = n.edges[:len(n.edges)-1]
}

func cloneDecoded(n *decodedNode) *decodedNode {
	out := &decodedNode{
		prefix: append([]byte(nil), n.prefix...),
		leaf:   n.leaf,
	}
	out.edges = make([]edge, len(n.edges))
	copy(out.edges, n.edges)
	return out
}

func concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
