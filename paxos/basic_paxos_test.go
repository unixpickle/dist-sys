package paxos

import (
	"testing"

	"github.com/unixpickle/dist-sys/simulator"
)

func TestBasicOneProposer(t *testing.T) {
	loop := simulator.NewEventLoop()
	nodes := []*simulator.Node{}
	ports := []*simulator.Port{}
	for i := 0; i < 3; i++ {
		nodes = append(nodes, simulator.NewNode())
		ports = append(ports, nodes[len(nodes)-1].Port(loop))
	}
	switcher := simulator.NewGreedyDropSwitcher(len(nodes), 1e6)
	network := simulator.NewSwitcherNetwork(switcher, nodes, 0)

	doneStream := loop.Stream()
	paxos := &BasicPaxos{Timeout: 10.0}

	// Run an acceptor on every node.
	for i := range nodes {
		idx := i
		loop.Go(func(h *simulator.Handle) {
			paxos.Accept(h, network, ports[idx], doneStream)
		})
	}

	// Run a proposer on one of the nodes.
	loop.Go(func(h *simulator.Handle) {
		value := paxos.Propose(h, network, nodes[0].Port(loop), ports, "goodbye world", 13)
		if value != "goodbye world" {
			t.Errorf("unexpected value: %s", value)
		}
		for range ports {
			h.Schedule(doneStream, nil, 0)
		}
	})

	loop.MustRun()
}

func TestBasicSlowProposer(t *testing.T) {
	// Make sure we try out many random seeds.
	for i := 0; i < 100; i++ {
		for _, randomized := range []bool{false, true} {
			for _, backoff := range []float64{0, 1} {
				loop := simulator.NewEventLoop()
				nodes := []*simulator.Node{}
				ports := []*simulator.Port{}
				for i := 0; i < 3; i++ {
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

				doneStream := loop.Stream()
				paxos := &BasicPaxos{Backoff: backoff, Timeout: 1.5}

				// Run an acceptor on every node.
				for i := range nodes {
					idx := i
					loop.Go(func(h *simulator.Handle) {
						paxos.Accept(h, network, ports[idx], doneStream)
					})
				}

				// A fast proposer that runs first.
				loop.Go(func(h *simulator.Handle) {
					value := paxos.Propose(h, network, nodes[0].Port(loop), ports, "goodbye world", 13)
					if value != "goodbye world" {
						t.Errorf("unexpected value: %s", value)
					}
				})

				// A slow proposer should accept the faster one.
				loop.Go(func(h *simulator.Handle) {
					h.Sleep(1e5)
					value := paxos.Propose(h, network, nodes[1].Port(loop), ports, "hello world", 11)
					if value != "goodbye world" {
						t.Errorf("unexpected value: %s", value)
					}
					for range ports {
						h.Schedule(doneStream, nil, 0)
					}
				})

				loop.MustRun()
			}
		}
	}
}

// TestExponentialBackoff tests the Basic Paxos backoff
// mechanism in a case where two proposers would normally
// compete forever.
func TestBasicBackoff(t *testing.T) {
	const NumNodes = 5
	const NumProposers = 3

	loop := simulator.NewEventLoop()
	nodes := []*simulator.Node{}
	ports := []*simulator.Port{}
	for i := 0; i < NumNodes; i++ {
		nodes = append(nodes, simulator.NewNode())
		ports = append(ports, nodes[len(nodes)-1].Port(loop))
	}

	// The network is slow enough that large accept
	// messages will take much longer than prepare
	// messages.
	switcher := simulator.NewGreedyDropSwitcher(len(nodes), 1.0)
	network := simulator.NewSwitcherNetwork(switcher, nodes, 0)

	doneStream := loop.Stream()
	paxos := &BasicPaxos{Backoff: 1.0, Timeout: 1e10}

	// Run an acceptor on every node.
	for i := range nodes {
		idx := i
		loop.Go(func(h *simulator.Handle) {
			paxos.Accept(h, network, ports[idx], doneStream)
		})
	}

	// Finish the acceptors once both proposers are done.
	proposerDones := loop.Stream()
	loop.Go(func(h *simulator.Handle) {
		for i := 0; i < NumProposers; i++ {
			h.Poll(proposerDones)
		}
		for range ports {
			h.Schedule(doneStream, nil, 0)
		}
	})

	// Run competing proposers.
	for i := 0; i < NumProposers; i++ {
		idx := i
		loop.Go(func(h *simulator.Handle) {
			value := paxos.Propose(h, network, nodes[idx].Port(loop), ports, "msg", 1000)
			if value != "msg" {
				t.Errorf("unexpected value: %s", value)
			}
			h.Schedule(proposerDones, nil, 0)
		})
	}

	loop.MustRun()
}
