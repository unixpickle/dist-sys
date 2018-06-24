package allreduce

// A NaiveAllreducer sends every gradient from every node
// to every other node.
type NaiveAllreducer struct{}

// Allreduce runs fn() on all of the nodes' vectors on
// every node.
func (n NaiveAllreducer) Allreduce(h *Host, data []float64, fn ReduceFn) []float64 {
	gatheredVecs := make([][]float64, len(h.Nodes))

	h.Bcast(data)

	for i := 0; i < len(gatheredVecs)-1; i++ {
		incoming, source := h.Recv()
		for j, node := range h.Nodes {
			if node == source {
				gatheredVecs[j] = incoming
			}
		}
	}

	for i, node := range h.Nodes {
		if node == h.Node {
			gatheredVecs[i] = data
		}
	}

	return fn(h.Handle, gatheredVecs...)
}
