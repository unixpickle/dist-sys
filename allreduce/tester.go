package allreduce

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/unixpickle/dist-sys/simulator"
)

// RunAllreducerTests runs a battery of tests on an
// Allreducer.
func RunAllreducerTests(t *testing.T, reducer Allreducer) {
	for _, numNodes := range []int{1, 2, 5, 15, 16, 17} {
		for _, size := range []int{0, 1337} {
			testName := fmt.Sprintf("Nodes=%d,Size=%d", numNodes, size)
			t.Run(testName, func(t *testing.T) {
				loop := simulator.NewEventLoop()
				vectors := make([][]float64, numNodes)
				nodes := make([]*simulator.Node, numNodes)
				sum := make([]float64, size)
				for i := range nodes {
					vectors[i] = make([]float64, size)
					for j := range vectors[i] {
						vectors[i][j] = rand.NormFloat64()
						sum[j] += vectors[i][j]
					}
					nodes[i] = &simulator.Node{Incoming: loop.Stream()}
				}

				switcher := simulator.NewGreedyDropSwitcher(numNodes, 1.0)
				network := simulator.NewSwitcherNetwork(switcher, nodes, 0.1)

				results := make([][]float64, numNodes)
				for i := range nodes {
					node := nodes[i]
					vec := vectors[i]
					nodeIdx := i
					loop.Go(func(h *simulator.Handle) {
						results[nodeIdx] = reducer.Allreduce(&Host{
							Handle:  h,
							Node:    node,
							Nodes:   nodes,
							Network: network,
						}, vec, Sum)
					})
				}

				if err := loop.Run(); err != nil {
					t.Fatal(err)
				}

				for i, res := range results[1:] {
					if len(res) != size {
						t.Errorf("result %d has length %d but expected %d", i, len(res), size)
						continue
					}
					for j, actual := range res {
						if actual != results[0][j] {
							t.Errorf("result %d is not identical to result 0", i)
							break
						}
					}
				}

				for i, x := range sum {
					if math.Abs(x-results[0][i]) > 1e-5 {
						t.Errorf("sum is incorrect (expected %f but got %f at component %d)",
							x, results[0][i], i)
						break
					}
				}
			})
		}
	}
}
