package allreduce

import "github.com/unixpickle/dist-sys/collcomm"

// A NaiveAllreducer sends every gradient from every node
// to every other node.
type NaiveAllreducer struct{}

// Allreduce runs fn() on all of the nodes' vectors on
// every node.
func (n NaiveAllreducer) Allreduce(c *collcomm.Comms, data []float64,
	fn collcomm.ReduceFn) []float64 {
	gatheredVecs := make([][]float64, len(c.Ports))

	c.Bcast(data)

	for i := 0; i < len(gatheredVecs)-1; i++ {
		incoming, source := c.Recv()
		gatheredVecs[c.IndexOf(source)] = incoming
	}

	gatheredVecs[c.Index()] = data

	return fn(c.Handle, gatheredVecs...)
}
