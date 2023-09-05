package raft

import (
	"testing"

	"github.com/unixpickle/dist-sys/simulator"
)

func TestRaftSimpleCase(t *testing.T) {
	t.Run("Randomized", func(t *testing.T) {
		testRaftSimpleCase(t, 5, true)
	})
	t.Run("Greedy", func(t *testing.T) {
		testRaftSimpleCase(t, 5, false)
	})
}

func testRaftSimpleCase(t *testing.T, numNodes int, randomized bool) {
	loop := simulator.NewEventLoop()
	nodes := []*simulator.Node{}
	ports := []*simulator.Port{}
	for i := 0; i < numNodes+1; i++ {
		nodes = append(nodes, simulator.NewNode())
		ports = append(ports, nodes[len(nodes)-1].Port(loop))
	}
	var network simulator.Network
	if randomized {
		network = simulator.RandomNetwork{}
	} else {
		switcher := simulator.NewGreedyDropSwitcher(len(nodes), 1e6)
		network = simulator.NewSwitcherNetwork(switcher, nodes, 0.1)
	}

	for i := 0; i < numNodes; i++ {
		index := i
		loop.Go(func(h *simulator.Handle) {
			var other []*simulator.Port
			var port *simulator.Port
			for i, p := range ports[:len(ports)-1] {
				if i == index {
					port = p
				} else {
					other = append(other, p)
				}
			}
			(&Raft[HashMapCommand, *HashMap]{
				Handle:  h,
				Network: network,
				Port:    port,
				Others:  other,
				Log: &Log[HashMapCommand, *HashMap]{
					Origin: &HashMap{},
				},
				ElectionTimeout:   30 + float64(i),
				HeartbeatInterval: 10,
			}).RunLoop()
		})
	}

	// Make sure we can actually push states.
	clientPort := ports[len(ports)-1]
	loop.Go(func(h *simulator.Handle) {
		// TODO: construct a client and send requests.
	})
}
