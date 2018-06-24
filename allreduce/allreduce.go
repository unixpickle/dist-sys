// Package allreduce implements algorithms for summing or
// maxing vectors across many different connected Nodes.
package allreduce

import "github.com/unixpickle/dist-sys/simulator"

// FlopTime is the amount of virtual time it takes to
// perform a single floating-point operation.
const FlopTime = 1e-9

// A ReduceFn is an operation that reduces many vectors
// into a single vector.
type ReduceFn func(h *simulator.Handle, vecs ...[]float64) []float64

// NetworkInfo stores information about the entire network
// from a single node's perspective.
type NetworkInfo struct {
	// Handle is the nodes' main Goroutine's handle on the
	// event loop.
	Handle *simulator.Handle

	// Node is the current node.
	Node *simulator.Node

	// Nodes contains all the nodes in the network,
	// including Node.
	Nodes []*simulator.Node

	// Network is the network connecting the nodes.
	Network *simulator.Network
}

// Allreducer is an algorithm that can apply a ReduceFn to
// vectors that are distributed across nodes.
type Allreducer interface {
	Allreduce(netInfo *NetworkInfo, data []float64, fn ReduceFn) []float64
}

// Sum is a ReduceFn that computes a vector sum.
func Sum(h *simulator.Handle, vecs ...[]float64) []float64 {
	for _, v := range vecs[1:] {
		if len(v) != len(vecs[0]) {
			panic("mismatching lengths")
		}
	}
	res := make([]float64, len(vecs[0]))
	for _, v := range vecs {
		for i, x := range v {
			res[i] += x
		}
	}

	// Simulate computation time.
	h.Sleep(FlopTime * float64(len(vecs)*len(vecs[0])))

	return res
}
