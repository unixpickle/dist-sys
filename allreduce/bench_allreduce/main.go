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

// Run creates a network and drops each host into its own
// Goroutine.
func (r *RunInfo) Run(loop *simulator.EventLoop, hostFn func(h *allreduce.Host)) {
	nodes := make([]*simulator.Node, r.NumNodes)
	ports := make([]*simulator.Port, r.NumNodes)
	for i := range nodes {
		nodes[i] = simulator.NewNode()
		ports[i] = nodes[i].Port(loop)
	}

	switcher := simulator.NewGreedyDropSwitcher(r.NumNodes, r.Rate)
	network := simulator.NewSwitcherNetwork(switcher, nodes, r.Latency)

	for i := range ports {
		port := ports[i]
		loop.Go(func(h *simulator.Handle) {
			hostFn(&allreduce.Host{
				Handle:  h,
				Port:    port,
				Ports:   ports,
				Network: network,
			})
		})
	}

	essentials.Must(loop.Run())
}

func main() {
	reducers := []allreduce.Allreducer{
		allreduce.NaiveAllreducer{},
		allreduce.TreeAllreducer{},
		allreduce.StreamAllreducer{},
	}
	reducerNames := []string{"Naive", "Tree", "Stream"}
	runs := []RunInfo{
		{
			NumNodes: 2,
			Latency:  0.1,
			Rate:     1e6,
		},
		{
			NumNodes: 16,
			Latency:  1e-3,
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
		{
			NumNodes: 32,
			Latency:  1e-4,
			Rate:     1e9,
		},
	}
	vecSizes := []int{10, 10000, 10000000}

	// Markdown table header.
	fmt.Print("| Nodes | Latency | NIC rate | Size ")
	for _, reducerName := range reducerNames {
		fmt.Printf("| %s ", reducerName)
	}
	fmt.Println("|")
	for i := 0; i < 4+len(reducers); i++ {
		fmt.Print("|:--")
	}
	fmt.Println("|")

	// Markdown table body.
	for _, runInfo := range runs {
		for _, size := range vecSizes {
			fmt.Printf(
				"| %d | %s | %s | %d ",
				runInfo.NumNodes,
				strconv.FormatFloat(runInfo.Latency, 'f', -1, 64),
				strconv.FormatFloat(runInfo.Rate, 'E', -1, 64),
				size,
			)
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
