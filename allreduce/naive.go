package allreduce

// A NaiveAllreducer sends every gradient from every node
// to every other node.
type NaiveAllreducer struct{}

// Allreduce runs fn() on all of the nodes' vectors on
// every node.
func (n NaiveAllreducer) Allreduce(h *Host, data []float64, fn ReduceFn) []float64 {
	gatheredVecs := make([][]float64, len(h.Ports))

	h.Bcast(data)

	for i := 0; i < len(gatheredVecs)-1; i++ {
		incoming, source := h.Recv()
		gatheredVecs[h.IndexOf(source)] = incoming
	}

	gatheredVecs[h.Index()] = data

	return fn(h.Handle, gatheredVecs...)
}
