// Package allreduce implements algorithms for summing or
// maxing vectors across many different connected Nodes.
package allreduce

// FlopTime is the amount of virtual time it takes to
// perform a single floating-point operation.
const FlopTime = 1e-9

// Allreducer is an algorithm that can apply a ReduceFn to
// vectors that are distributed across nodes.
//
// It is not necessarily safe to call Allreduce() multiple
// times in a row in the same Nodes.
// In particular, it is not guaranteed that every
// Allreduce() call finishes at the same time, so certain
// nodes may finish while packets are still in flight for
// other nodes.
type Allreducer interface {
	Allreduce(h *Host, data []float64, fn ReduceFn) []float64
}
