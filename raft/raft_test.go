package raft

import (
	"context"
	"strconv"
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

	// The network is always ordered, but may have random latency.
	var latency float64
	if randomized {
		latency = 0.1
	} else {
		latency = 0.0
	}
	network := simulator.NewOrderedNetwork(1e6, latency)

	context, cancelFn := context.WithCancel(context.Background())

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
				Context: context,
				Handle:  h,
				Network: network,
				Port:    port,
				Others:  other,
				Log: &Log[HashMapCommand, *HashMap]{
					Origin: &HashMap{},
				},
				ElectionTimeout:   30 + float64(index)*2,
				HeartbeatInterval: 10,
			}).RunLoop()
		})
	}

	// Make sure we can actually push states.
	clientPort := ports[len(ports)-1]
	loop.Go(func(h *simulator.Handle) {
		defer cancelFn()
		client := &Client[HashMapCommand]{
			Handle:      h,
			Network:     network,
			Port:        clientPort,
			Servers:     ports[:len(ports)-1],
			SendTimeout: 10,
		}
		for i := 0; i < 10; i++ {
			value := "hello" + strconv.Itoa(i)
			x, err := client.Send(HashMapCommand{Key: strconv.Itoa(i), Value: value}, 0)
			if err != nil {
				t.Fatal(err)
			} else if v := x.(StringResult).Value; v != value {
				t.Fatalf("expected %#v but got %#v", v, x)
			}
			for j := 0; j <= i; j++ {
				expected := "hello" + strconv.Itoa(i)
				resp, err := client.Send(HashMapCommand{Key: strconv.Itoa(i)}, 0)
				if err != nil {
					t.Fatal(err)
				} else if v := resp.(StringResult).Value; v != expected {
					t.Fatalf("expected %#v but got %#v", expected, v)
				}
			}
		}
	})

	loop.MustRun()
}
