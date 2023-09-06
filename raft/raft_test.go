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
