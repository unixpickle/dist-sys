// Package allreduce implements algorithms for summing or
// maxing vectors across many different connected Nodes.
package allreduce

import "github.com/unixpickle/dist-sys/collcomm"

// Allreducer is an algorithm that can apply a ReduceFn to
// vectors that are distributed across nodes.
//
// It is not safe to call Allreduce() multiple times in a
// row with the same Comms object.
// A new set of ports must be used every time to avoid
// interference.
type Allreducer interface {
	Allreduce(c *collcomm.Comms, data []float64, fn collcomm.ReduceFn) []float64
}
