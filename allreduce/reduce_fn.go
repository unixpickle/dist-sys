package allreduce

import (
	"github.com/unixpickle/dist-sys/simulator"
)

// A ReduceFn is an operation that reduces many vectors
// into a single vector.
type ReduceFn func(h *simulator.Handle, vecs ...[]float64) []float64

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
