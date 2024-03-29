package simulator

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
)

func ExampleNetwork() {
	loop := NewEventLoop()

	// A switch with two ports that do I/O at 2 bytes/sec.
	switcher := NewGreedyDropSwitcher(2, 2.0)

	node1 := NewNode()
	node2 := NewNode()
	latency := 0.25
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, latency)
	port1 := node1.Port(loop)
	port2 := node2.Port(loop)

	// Goroutine for node 1.
	loop.Go(func(h *Handle) {
		message := port1.Recv(h).Message.(string)
		response := strings.ToUpper(message)

		// Simulate time it took to do the calculation.
		h.Sleep(0.125)

		network.Send(h, &Message{
			Source:  port1,
			Dest:    port2,
			Message: response,
			Size:    float64(len(message)),
		})
	})

	// Goroutine for node 2.
	loop.Go(func(h *Handle) {
		msg := "this should be capitalized"
		network.Send(h, &Message{
			Source:  port2,
			Dest:    port1,
			Message: msg,
			Size:    float64(len(msg)),
		})
		response := port2.Recv(h).Message.(string)
		fmt.Println(response, h.Time())
	})

	loop.MustRun()

	// Output: THIS SHOULD BE CAPITALIZED 26.625
}

func TestSwitchedNetworkSingleMessage(t *testing.T) {
	loop := NewEventLoop()

	switcher := NewGreedyDropSwitcher(2, 2.0)
	node1 := NewNode()
	node2 := NewNode()
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, 3.0)
	port1 := node1.Port(loop)
	port2 := node2.Port(loop)

	fmt.Println(node1 == node2)

	loop.Go(func(h *Handle) {
		network.Send(h, &Message{
			Source:  port1,
			Dest:    port2,
			Message: "hi node 2",
			Size:    124.0,
		})
		if val := port1.Recv(h).Message; val != "hi node 1" {
			t.Errorf("unexpected message: %s", val)
		}
	})
	loop.Go(func(h *Handle) {
		network.Send(h, &Message{
			Source:  port2,
			Dest:    port1,
			Message: "hi node 1",
			Size:    124.0,
		})
		if val := port2.Recv(h).Message; val != "hi node 2" {
			t.Errorf("unexpected message: %s", val)
		}
	})

	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	expectedTime := 124.0/2.0 + 3.0
	if loop.Time() != expectedTime {
		t.Errorf("time should be %f but got %f", expectedTime, loop.Time())
	}
}

func TestSwitchedNetworkOversubscribed(t *testing.T) {
	loop := NewEventLoop()

	dataRate := 4.0
	switcher := NewGreedyDropSwitcher(2, dataRate)
	node1 := NewNode()
	node2 := NewNode()
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, 2.0)
	port1 := node1.Port(loop)
	port2 := node2.Port(loop)

	loop.Go(func(h *Handle) {
		network.Send(h, &Message{
			Source:  port1,
			Dest:    port2,
			Message: "hi node 2 (message 1)",
			Size:    123.0,
		})
		network.Send(h, &Message{
			Source:  port1,
			Dest:    port2,
			Message: "hi node 2 (message 2)",
			Size:    124.0,
		})
		if val := port1.Recv(h).Message; val != "hi node 1" {
			t.Errorf("unexpected message: %s", val)
		}
		expectedTime := 1.0 + 2.0 + 124.0/dataRate
		if h.Time() != expectedTime {
			t.Errorf("expected time %f but got %f", expectedTime, h.Time())
		}
	})

	loop.Go(func(h *Handle) {
		// Make sure the other messages are in-flight.
		// This helps us test for the fact that we can
		// reschedule a message before the other messages.
		h.Sleep(1)

		network.Send(h, &Message{
			Source:  port2,
			Dest:    port1,
			Message: "hi node 1",
			Size:    124.0,
		})
		if val := port2.Recv(h).Message; val != "hi node 2 (message 1)" {
			t.Errorf("unexpected message: %s", val)
		}
		expectedTime := 2.0 + 2.0*123.0/dataRate
		if h.Time() != expectedTime {
			t.Errorf("expected time %f but got %f", expectedTime, h.Time())
		}
		if val := port2.Recv(h).Message; val != "hi node 2 (message 2)" {
			t.Errorf("unexpected message: %s", val)
		}
		expectedTime += 1.0 / dataRate
		if h.Time() != expectedTime {
			t.Errorf("expected time %f but got %f", expectedTime, h.Time())
		}
	})

	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	expectedTime := 2.0 + 2.0*123.0/dataRate + 1.0/dataRate
	if loop.Time() != expectedTime {
		t.Errorf("time should be %f but got %f", expectedTime, loop.Time())
	}

	// Make sure that there are no stray messages.
	for _, port := range []*Port{port1, port2} {
		loop.Go(func(h *Handle) {
			h.Poll(port.Incoming)
		})
		if loop.Run() == nil {
			t.Error("expected deadlock error")
		}
	}
}

func TestSwitchedNetworkBatchedEquivalence(t *testing.T) {
	loop := NewEventLoop()

	dataRate := 4.0
	switcher := NewGreedyDropSwitcher(2, dataRate)
	node1 := NewNode()
	node2 := NewNode()
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, 2.0)

	testBatchedEquivalence(t, loop, network, node1.Port(loop), node2.Port(loop))
}

func testBatchedEquivalence(t *testing.T, loop *EventLoop, network Network, p1, p2 *Port) {
	messages := []*Message{}
	for i := 0; i < 20; i++ {
		messages = append(messages, &Message{
			Source:  p1,
			Dest:    p2,
			Message: rand.NormFloat64(),
			Size:    rand.Float64() + 0.1,
		})
	}

	var serialMessages []*Message
	var serialTimes []float64
	loop.Go(func(h *Handle) {
		for _, msg := range messages {
			network.Send(h, msg)
		}
		for range messages {
			serialMessages = append(serialMessages, p2.Recv(h))
			serialTimes = append(serialTimes, h.Time())
		}
	})
	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	t1 := loop.Time()

	loop.Go(func(h *Handle) {
		network.Send(h, messages...)
		startTime := h.Time()
		for i := range messages {
			msg := p2.Recv(h)
			if serialMessages[i] != msg {
				t.Errorf("msg %d: expected %v but got %v", i, serialMessages[i], msg)
			}
			curTime := h.Time() - startTime
			if math.Abs(curTime-serialTimes[i])/curTime > 1e-5 {
				t.Errorf("msg %d: expected time %f but got %f", i, serialTimes[i], curTime)
			}
		}
	})
	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	t2 := loop.Time()

	if math.Abs(t2-2*t1)/t1 > 1e-5 {
		t.Errorf("expected end time %f but got %f", 2*t1, t2)
	}
}

func BenchmarkSwitchedNetworkSends(b *testing.B) {
	for i := 0; i < b.N; i++ {
		loop := NewEventLoop()
		nodes := make([]*Node, 8)
		ports := make([]*Port, 8)
		for i := range nodes {
			nodes[i] = NewNode()
			ports[i] = nodes[i].Port(loop)
		}
		switcher := NewGreedyDropSwitcher(len(nodes), 1.0)
		network := NewSwitcherNetwork(switcher, nodes, 0.1)
		for j := range ports {
			port := ports[j]
			loop.Go(func(h *Handle) {
				for _, other := range ports {
					network.Send(h, &Message{
						Source:  port,
						Dest:    other,
						Message: "hello",
						Size:    rand.Float64(),
					})
				}
			})
		}
		if err := loop.Run(); err != nil {
			b.Fatal(err)
		}
	}
}
