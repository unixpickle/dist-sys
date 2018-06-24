package allreduce

// A NaiveAllreducer sends every gradient from every node
// to every other node.
type NaiveAllreducer struct{}

// Allreduce runs fn() on all of the nodes' vectors on
// every node.
func (n NaiveAllreducer) Allreduce(h *Host, data []float64, fn ReduceFn) []float64 {
	gatheredVecs := make([][]float64, len(h.Nodes))

	for i, node := range h.Nodes {
		if node != h.Node {
			h.Send(node, data)
		} else {
			gatheredVecs[i] = data
		}
	}

	for i := 0; i < len(gatheredVecs)-1; i++ {
		incoming, source := h.Recv()
		for j, node := range h.Nodes {
			if node == source {
				gatheredVecs[j] = incoming
			}
		}
	}

	return fn(h.Handle, gatheredVecs...)
}
