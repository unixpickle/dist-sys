package raft

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/unixpickle/dist-sys/simulator"
)

func TestRaftSimpleCase(t *testing.T) {
	t.Run("Latency", func(t *testing.T) {
		testRaftSimpleCase(t, 5, true)
	})
	t.Run("Instant", func(t *testing.T) {
		testRaftSimpleCase(t, 5, false)
	})
}

func testRaftSimpleCase(t *testing.T, numNodes int, randomized bool) {
	env := NewRaftEnvironment(5, 1, randomized)
	env.Loop.Go(func(h *simulator.Handle) {
		defer env.Cancel()
		client := &Client[HashMapCommand]{
			Handle:      h,
			Network:     env.Network,
			Port:        env.Clients[0],
			Servers:     env.Servers,
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

	env.Loop.MustRun()
}

func TestRaftNodeFailures(t *testing.T) {
	t.Run("Latency", func(t *testing.T) {
		testRaftNodeFailures(t, 5, true)
	})
	t.Run("Instant", func(t *testing.T) {
		testRaftNodeFailures(t, 5, false)
	})
}

func testRaftNodeFailures(t *testing.T, numNodes int, randomized bool) {
	env := NewRaftEnvironment(5, 1, randomized)
	env.Loop.Go(func(h *simulator.Handle) {
		defer env.Cancel()
		client := &Client[HashMapCommand]{
			Handle:      h,
			Network:     env.Network,
			Port:        env.Clients[0],
			Servers:     env.Servers,
			SendTimeout: 10,
		}
		downMachine := -1
		for i := 0; i < 20; i++ {
			value := "hello" + strconv.Itoa(i)
			x, err := client.Send(HashMapCommand{Key: strconv.Itoa(i), Value: value}, 0)
			if err != nil {
				t.Fatal(err)
			} else if v := x.(StringResult).Value; v != value {
				t.Fatalf("expected %#v but got %#v", v, x)
			}
			for j := 0; j <= i; j++ {
				// With some probability, we bring a machine down or back up,
				// then wait a bit for it to potentially fail.
				if rand.Intn(2) == 0 {
					if downMachine == -1 {
						downMachine = rand.Intn(len(env.Servers))
						env.Network.SetDown(h, env.Servers[downMachine].Node, true)
					} else {
						env.Network.SetDown(h, env.Servers[downMachine].Node, false)
						downMachine = -1
					}
					h.Sleep(rand.Float64() * 45)
				}

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

	env.Loop.MustRun()
}

func TestRaftMultiClientNodeFailures(t *testing.T) {
	t.Run("Latency", func(t *testing.T) {
		testRaftMultiClientNodeFailures(t, 5, true)
	})
	t.Run("Instant", func(t *testing.T) {
		testRaftMultiClientNodeFailures(t, 5, false)
	})
}

func testRaftMultiClientNodeFailures(t *testing.T, numNodes int, randomized bool) {
	env := NewRaftEnvironment(5, 2, randomized)

	var wg sync.WaitGroup
	for i, port := range env.Clients {
		wg.Add(1)
		keyPrefix := fmt.Sprintf("rank%d-", i)
		env.Loop.Go(func(h *simulator.Handle) {
			defer wg.Done()
			client := &Client[HashMapCommand]{
				Handle:      h,
				Network:     env.Network,
				Port:        port,
				Servers:     env.Servers,
				SendTimeout: 10,
			}
			downMachine := -1
			for i := 0; i < 20; i++ {
				value := "hello" + strconv.Itoa(i)
				x, err := client.Send(HashMapCommand{Key: keyPrefix + strconv.Itoa(i), Value: value}, 0)
				if err != nil {
					t.Fatal(err)
				} else if v := x.(StringResult).Value; v != value {
					t.Fatalf("expected %#v but got %#v", v, x)
				}
				for j := 0; j <= i; j++ {
					// With some probability, we bring a machine down or back up,
					// then wait a bit for it to potentially fail.
					if rand.Intn(2) == 0 {
						if downMachine == -1 {
							downMachine = rand.Intn(len(env.Servers))
							env.Network.SetDown(h, env.Servers[downMachine].Node, true)
						} else {
							env.Network.SetDown(h, env.Servers[downMachine].Node, false)
							downMachine = -1
						}
						h.Sleep(rand.Float64() * 45)
					}

					expected := "hello" + strconv.Itoa(i)
					resp, err := client.Send(HashMapCommand{Key: keyPrefix + strconv.Itoa(i)}, 0)
					if err != nil {
						t.Fatal(err)
					} else if v := resp.(StringResult).Value; v != expected {
						t.Fatalf("expected %#v but got %#v", expected, v)
					}
				}
			}
		})
	}

	go func() {
		wg.Wait()
		env.Cancel()
	}()

	env.Loop.MustRun()
}

func TestRaftMultiClientInterleavedNodeFailures(t *testing.T) {
	env := NewRaftEnvironment(5, 2, true)

	// Increase random latency to cause splits / partial sends.
	env.Network.MaxRandomLatency = 1.0

	var wg sync.WaitGroup
	for i, port := range env.Clients {
		wg.Add(1)
		keyPrefix := fmt.Sprintf("rank%d-", i)
		env.Loop.Go(func(h *simulator.Handle) {
			defer wg.Done()
			client := &Client[HashMapCommand]{
				Handle:  h,
				Network: env.Network,
				Port:    port,
				Servers: env.Servers,

				// With lower values, servers seem to get overloaded.
				SendTimeout: 30,
			}
			for i := 0; i < 50; i++ {
				value := "hello" + strconv.Itoa(i)
				x, err := client.Send(HashMapCommand{Key: keyPrefix + strconv.Itoa(i), Value: value}, 0)
				if err != nil {
					t.Fatal(err)
				} else if v := x.(StringResult).Value; v != value {
					t.Fatalf("expected %#v but got %#v", v, x)
				}
				for j := 0; j <= i; j++ {
					expected := "hello" + strconv.Itoa(i)
					resp, err := client.Send(HashMapCommand{Key: keyPrefix + strconv.Itoa(i)}, 0)
					if err != nil {
						t.Fatal(err)
					} else if v := resp.(StringResult).Value; v != expected {
						t.Fatalf("expected %#v but got %#v", expected, v)
					}
				}
				h.Sleep(rand.Float64() * 10)
			}
		})
	}

	// Randomly bring down at most two servers at once.
	for i := 0; i < 2; i++ {
		env.Loop.Go(func(h *simulator.Handle) {
			for {
				select {
				case <-env.Context.Done():
					return
				default:
				}
				h.Sleep(rand.Float64() * 30)
				node := rand.Intn(len(env.Servers))
				env.Network.SetDown(h, env.Servers[node].Node, true)
				h.Sleep(rand.Float64() * 120)
				env.Network.SetDown(h, env.Servers[node].Node, false)
				h.Sleep(rand.Float64()*60 + 60)
			}
		})
	}

	go func() {
		wg.Wait()
		env.Cancel()
	}()

	env.Loop.MustRun()
}

func TestRaftCommitOlderTerm(t *testing.T) {
	env := NewRaftEnvironment(5, 1, false)

	env.Loop.Go(func(h *simulator.Handle) {
		defer env.Cancel()

		clientPort := env.Clients[0]

		findLeader := func() *simulator.Port {
			var result *simulator.Port
			env.Network.Sniff(h, func(msg *simulator.Message) bool {
				if rm, ok := msg.Message.(*RaftMessage[HashMapCommand, *HashMap]); ok {
					if rm.AppendLogs != nil {
						result = msg.Source
						return false
					}
				}
				return true
			})
			return result
		}

		bumpLeaderToTerm := func(leaderPort *simulator.Port, term int64) {
			// Send a bogus packet to the leader before it's disconnected
			// so that it starts voting with very high terms.
			msg := &RaftMessage[HashMapCommand, *HashMap]{
				Vote: &Vote{Term: term},
			}
			var follower *simulator.Port
			for _, port := range env.Servers {
				if port != leaderPort && !env.Network.IsDown(port.Node) {
					follower = port
					break
				}
			}
			env.Network.SendInstantly(h, &simulator.Message{
				Source:  follower,
				Dest:    leaderPort,
				Message: msg,
				Size:    float64(msg.Size()),
			})
			env.Network.SendInstantly(h)
		}

		// Wait until a leader is almost certainly alive.
		h.Sleep(120)

		leader := findLeader()

		msg := &CommandMessage[HashMapCommand]{
			Command: HashMapCommand{Key: "key1", Value: "value1"},
			ID:      "id1",
		}
		env.Network.SendInstantly(h, &simulator.Message{
			Source:  clientPort,
			Dest:    leader,
			Message: msg,
			Size:    float64(msg.Command.Size() + len(msg.ID)),
		})
		h.Sleep(1e-8)
		// Make sure leader will win next election.
		bumpLeaderToTerm(leader, 10)
		h.Sleep(1e-8)
		env.Network.SetDown(h, leader.Node, true)

		// A new leader should exist which doesn't know about our
		// previous log entry.
		h.Sleep(120)
		newLeader := findLeader()

		// Same deal: give them an entry only they know about.
		msg = &CommandMessage[HashMapCommand]{
			Command: HashMapCommand{Key: "key2", Value: "value2"},
			ID:      "id2",
		}
		env.Network.SendInstantly(h, &simulator.Message{
			Source:  clientPort,
			Dest:    newLeader,
			Message: msg,
			Size:    float64(msg.Command.Size() + len(msg.ID)),
		})
		h.Sleep(1e-8)
		bumpLeaderToTerm(newLeader, 100)
		h.Sleep(1e-8)
		env.Network.SetDown(h, newLeader.Node, true)
		env.Network.SetDown(h, leader.Node, false)

		// Give original leader a chance to erroneously commit
		// if it is incorrectly implemented.
		h.Sleep(120)
		newNewLeader := findLeader()
		if newNewLeader != leader {
			t.Fatal("expected to obtain original leader")
		}

		// Bring back up the second leader.
		env.Network.SetDown(h, newLeader.Node, false)
		env.Network.SetDown(h, leader.Node, true)

		h.Sleep(120)
		newNewNewLeader := findLeader()
		if newNewNewLeader != newLeader {
			t.Fatal("unexpected leader after long downtime (second leader)")
		}

		// Make sure the key from newLeader is committed
		// and not the previous one.
		client := &Client[HashMapCommand]{
			Handle:  h,
			Network: env.Network,
			Port:    clientPort,
			Servers: env.Servers,

			// With lower values, servers seem to get overloaded.
			SendTimeout: 30,
		}
		res, _ := client.Send(HashMapCommand{Key: "key1"}, 0)
		if res.(StringResult).Value != "" {
			t.Fatalf("unexpected key1 value: %#v", res)
		}
		res, _ = client.Send(HashMapCommand{Key: "key2"}, 0)
		if res.(StringResult).Value != "value2" {
			t.Fatalf("unexpected key2 value: %#v", res)
		}
	})

	env.Loop.MustRun()
}

type RaftEnvironment struct {
	Cancel  func()
	Context context.Context
	Servers []*simulator.Port
	Clients []*simulator.Port
	Loop    *simulator.EventLoop
	Network *simulator.OrderedNetwork
}

func NewRaftEnvironment(numServers, numClients int, randomized bool) *RaftEnvironment {
	loop := simulator.NewEventLoop()
	nodes := []*simulator.Node{}
	ports := []*simulator.Port{}
	for i := 0; i < numServers+numClients; i++ {
		node := simulator.NewNode()
		nodes = append(nodes, node)
		ports = append(ports, node.Port(loop))
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

	for i := 0; i < numServers; i++ {
		index := i
		loop.Go(func(h *simulator.Handle) {
			var other []*simulator.Port
			var port *simulator.Port
			for i, p := range ports[:len(ports)-numClients] {
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

	return &RaftEnvironment{
		Cancel:  cancelFn,
		Context: context,
		Servers: ports[:numServers],
		Clients: ports[numServers:],
		Loop:    loop,
		Network: network,
	}
}
