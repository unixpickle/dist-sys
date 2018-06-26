package allreduce

import "github.com/unixpickle/dist-sys/simulator"

// A TreeAllreducer arranges the Ports in a binary tree
// and performs a reduction by going up the three to a
// root node, and then back down the tree to a leaf.
type TreeAllreducer struct{}

// Allreduce calls fn on vectors along a tree and returns
// the resulting reduced vector.
func (t TreeAllreducer) Allreduce(h *Host, data []float64, fn ReduceFn) []float64 {
	parent, children := positionInTree(h)

	messages := [][]float64{data}
	for _ = range children {
		msg, _ := h.Recv()
		messages = append(messages, msg)
	}

	finalVector := fn(h.Handle, messages...)
	if parent != nil {
		h.Send(parent, finalVector)
		finalVector, _ = h.Recv()
	}

	for _, child := range children {
		h.Send(child, finalVector)
	}

	return finalVector
}

// positionInTree returns the child Ports and parent node
// for a host in the reduction tree.
//
// There may be no children.
// There may be no parent (for the root node).
func positionInTree(h *Host) (parent *simulator.Port, children []*simulator.Port) {
	idx := h.Index()
	for depth := uint(0); true; depth++ {
		rowSize := 1 << depth
		rowStart := rowSize - 1
		if idx >= rowStart+rowSize {
			continue
		}
		rowIdx := idx - rowStart
		if depth > 0 {
			parent = h.Ports[rowIdx/2+(rowSize/2-1)]
		}
		firstChild := rowIdx*2 + (rowSize*2 - 1)
		for i := 0; i < 2; i++ {
			if firstChild+i < len(h.Ports) {
				children = append(children, h.Ports[firstChild+i])
			}
		}
		return
	}
	panic("unreachable")
}
