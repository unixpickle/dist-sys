package main

import (
	"fmt"
	"strconv"

	"github.com/unixpickle/dist-sys/allreduce"
	"github.com/unixpickle/dist-sys/simulator"
	"github.com/unixpickle/essentials"
)

// RunInfo describes a specific network configuration.
type RunInfo struct {
	NumNodes int
	Latency  float64
	Rate     float64
}

func (r *RunInfo) String() string {
	return fmt.Sprintf("%d nodes, latency=%s, rate=%s", r.NumNodes, strconv.FormatFloat(r.Latency, 'f', -1, 64),
		strconv.FormatFloat(r.Rate, 'E', -1, 64))
}

// Run creates a network and drops each host into its own
// Goroutine.
func (r *RunInfo) Run(loop *simulator.EventLoop, hostFn func(h *allreduce.Host)) {
	nodes := make([]*simulator.Node, r.NumNodes)
	for i := range nodes {
		nodes[i] = &simulator.Node{Incoming: loop.Stream()}
	}

	switcher := simulator.NewGreedyDropSwitcher(r.NumNodes, r.Rate)
	network := simulator.NewSwitcherNetwork(switcher, nodes, r.Latency)

	for i := range nodes {
		node := nodes[i]
		loop.Go(func(h *simulator.Handle) {
			hostFn(&allreduce.Host{
				Handle:  h,
				Node:    node,
				Nodes:   nodes,
				Network: network,
			})
		})
	}

	essentials.Must(loop.Run())
}

func main() {
	reducers := []allreduce.Allreducer{
		allreduce.NaiveAllreducer{},
	}
	reducerNames := []string{"NaiveAllreducer"}
	runs := []RunInfo{
		{
			NumNodes: 2,
			Latency:  0.1,
			Rate:     1e6,
		},
		{
			NumNodes: 32,
			Latency:  0.1,
			Rate:     1e6,
		},
		{
			NumNodes: 32,
			Latency:  0.1,
			Rate:     1e9,
		},
	}
	vecSizes := []int{10, 10000, 10000000}

	// Markdown table header.
	fmt.Print("| Scenario ")
	for _, reducerName := range reducerNames {
		fmt.Printf("| %s ", reducerName)
	}
	fmt.Println("|")
	for _ = range reducerNames {
		fmt.Print("|:--")
	}
	fmt.Println("|")

	// Markdown table body.
	for _, runInfo := range runs {
		for _, size := range vecSizes {
			fmt.Printf("| %s (%d dims) ", runInfo.String(), size)
			for _, reducer := range reducers {
				loop := simulator.NewEventLoop()
				runInfo.Run(loop, func(h *allreduce.Host) {
					vec := make([]float64, size)
					reducer.Allreduce(h, vec, FakeReduce)
				})
				fmt.Printf("| %f ", loop.Time())
			}
			fmt.Println("|")
		}
	}
}

// FakeReduce is a ReduceFn that takes no actual CPU time.
func FakeReduce(h *simulator.Handle, vecs ...[]float64) []float64 {
	h.Sleep(allreduce.FlopTime * float64(len(vecs)*len(vecs[0])))
	return make([]float64, len(vecs[0]))
}
