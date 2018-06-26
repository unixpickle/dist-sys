package allreduce

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/unixpickle/dist-sys/collcomm"

	"github.com/unixpickle/dist-sys/simulator"
)

// RunAllreducerTests runs a battery of tests on an
// Allreducer.
func RunAllreducerTests(t *testing.T, reducer Allreducer) {
	for _, numNodes := range []int{1, 2, 5, 15, 16, 17} {
		for _, size := range []int{0, 1337} {
			for _, randomized := range []bool{false, true} {
				testName := fmt.Sprintf("Nodes=%d,Size=%d,Random=%v", numNodes, size, randomized)
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
						nodes[i] = simulator.NewNode()
					}

					var network simulator.Network
					if randomized {
						network = simulator.RandomNetwork{}
					} else {
						switcher := simulator.NewGreedyDropSwitcher(numNodes, 1.0)
						network = simulator.NewSwitcherNetwork(switcher, nodes, 0.1)
					}

					results := make([][]float64, numNodes)
					collcomm.SpawnComms(loop, network, nodes, func(c *collcomm.Comms) {
						results[c.Index()] = reducer.Allreduce(c, vectors[c.Index()], collcomm.Sum)
					})

					if err := loop.Run(); err != nil {
						t.Fatal(err)
					}

					verifyReductionResults(t, results, sum)
				})
			}
		}
	}
}

func verifyReductionResults(t *testing.T, results [][]float64, expected []float64) {
	for i, res := range results[1:] {
		if len(res) != len(expected) {
			t.Errorf("result %d has length %d but expected %d", i, len(res), len(expected))
			continue
		}
		for j, actual := range res {
			if actual != results[0][j] {
				t.Errorf("result %d is not identical to result 0", i)
				break
			}
		}
	}

	for i, x := range expected {
		if math.Abs(x-results[0][i]) > 1e-5 {
			t.Errorf("sum is incorrect (expected %f but got %f at component %d)",
				x, results[0][i], i)
			break
		}
	}
}
