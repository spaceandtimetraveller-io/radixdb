package radixdb

import (
	"encoding/binary"
	"sort"
)

type decodedNode struct {
	prefix []byte
	edges  []edge
	leaf   Ref
}

type edge struct {
	label byte
	child Ref
}

const nodeHdr = 16

func nodeSize(prefixLen int, nChild int) int {
	return nodeHdr + prefixLen + nChild*16
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
	binary.LittleEndian.PutUint64(b[8:16], uint64(n.leaf))
	copy(b[nodeHdr:], n.prefix)
	off := nodeHdr + len(n.prefix)
	edges := append([]edge(nil), n.edges...)
	sort.Slice(edges, func(i, j int) bool { return edges[i].label < edges[j].label })
	for _, e := range edges {
		b[off] = e.label
		off += 8
		binary.LittleEndian.PutUint64(b[off:off+8], uint64(e.child))
		off += 8
	}
	return b
}

// decodeNodeInto decodes a node at offset at into n, reusing n.edges capacity when possible.
func decodeNodeInto(n *decodedNode, data []byte, at int) error {
	if at < 0 || at+nodeHdr > len(data) {
		return ErrCorrupt
	}
	pl := int(binary.LittleEndian.Uint16(data[at : at+2]))
	nc := int(binary.LittleEndian.Uint16(data[at+2 : at+4]))
	hasLeaf := data[at+4]
	leafOff := Ref(binary.LittleEndian.Uint64(data[at+8 : at+16]))
	if pl > len(data)-at-nodeHdr {
		return ErrCorrupt
	}
	need := nodeHdr + pl + nc*16
	if at+need > len(data) {
		return ErrCorrupt
	}
	n.prefix = data[at+nodeHdr : at+nodeHdr+pl]
	off := at + nodeHdr + pl
	if cap(n.edges) < nc {
		n.edges = make([]edge, nc)
	} else {
		n.edges = n.edges[:nc]
	}
	for i := 0; i < nc; i++ {
		n.edges[i].label = data[off]
		off += 8
		n.edges[i].child = Ref(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
	}
	n.leaf = 0
	if hasLeaf != 0 {
		n.leaf = leafOff
	}
	return nil
}

func decodeNode(data []byte, at int) (*decodedNode, error) {
	n := &decodedNode{}
	if err := decodeNodeInto(n, data, at); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *decodedNode) getEdge(label byte) (int, *decodedNode, Ref) {
	idx := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if idx < len(n.edges) && n.edges[idx].label == label {
		return idx, nil, n.edges[idx].child
	}
	return -1, nil, 0
}

func (n *decodedNode) replaceEdge(label byte, newRef Ref) {
	idx := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if idx >= len(n.edges) || n.edges[idx].label != label {
		panic("radixdb: replace missing edge")
	}
	n.edges[idx].child = newRef
}

func (n *decodedNode) addEdge(label byte, child Ref) {
	n.edges = append(n.edges, edge{label: label, child: child})
	sort.Slice(n.edges, func(i, j int) bool { return n.edges[i].label < n.edges[j].label })
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
