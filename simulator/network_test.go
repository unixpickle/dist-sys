package simulator

import (
	"fmt"
	"strings"
	"testing"
)

func ExampleNetwork() {
	loop := NewEventLoop()

	// A switch with two ports that do I/O at 2 bytes/sec.
	switcher := NewGreedyDropSwitcher(2, 2.0)

	node1 := &Node{Incoming: loop.Stream()}
	node2 := &Node{Incoming: loop.Stream()}
	latency := 0.25
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, latency)

	// Goroutine for node 1.
	loop.Go(func(h *Handle) {
		message := node1.Recv(h).Message.(string)
		response := strings.ToUpper(message)

		// Simulate time it took to do the calculation.
		h.Sleep(0.125)

		network.Send(h, &Message{
			Source:  node1,
			Dest:    node2,
			Message: response,
			Size:    float64(len(message)),
		})
	})

	// Goroutine for node 2.
	loop.Go(func(h *Handle) {
		msg := "this should be capitalized"
		network.Send(h, &Message{
			Source:  node2,
			Dest:    node1,
			Message: msg,
			Size:    float64(len(msg)),
		})
		response := node2.Recv(h).Message.(string)
		fmt.Println(response, h.Time())
	})

	loop.Run()

	// Output: THIS SHOULD BE CAPITALIZED 26.625
}

func TestSwitchedNetworkSingleMessage(t *testing.T) {
	loop := NewEventLoop()

	switcher := NewGreedyDropSwitcher(2, 2.0)
	node1 := &Node{Incoming: loop.Stream()}
	node2 := &Node{Incoming: loop.Stream()}
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, 3.0)

	loop.Go(func(h *Handle) {
		network.Send(h, &Message{
			Source:  node1,
			Dest:    node2,
			Message: "hi node 2",
			Size:    124.0,
		})
		if val := node1.Recv(h).Message; val != "hi node 1" {
			t.Errorf("unexpected message: %s", val)
		}
	})
	loop.Go(func(h *Handle) {
		network.Send(h, &Message{
			Source:  node2,
			Dest:    node1,
			Message: "hi node 1",
			Size:    124.0,
		})
		if val := node2.Recv(h).Message; val != "hi node 2" {
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
	node1 := &Node{Incoming: loop.Stream()}
	node2 := &Node{Incoming: loop.Stream()}
	network := NewSwitcherNetwork(switcher, []*Node{node1, node2}, 2.0)

	loop.Go(func(h *Handle) {
		network.Send(h, &Message{
			Source:  node1,
			Dest:    node2,
			Message: "hi node 2 (message 1)",
			Size:    123.0,
		})
		network.Send(h, &Message{
			Source:  node1,
			Dest:    node2,
			Message: "hi node 2 (message 2)",
			Size:    124.0,
		})
		if val := node1.Recv(h).Message; val != "hi node 1" {
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
			Source:  node2,
			Dest:    node1,
			Message: "hi node 1",
			Size:    124.0,
		})
		if val := node2.Recv(h).Message; val != "hi node 2 (message 1)" {
			t.Errorf("unexpected message: %s", val)
		}
		expectedTime := 2.0 + 2.0*123.0/dataRate
		if h.Time() != expectedTime {
			t.Errorf("expected time %f but got %f", expectedTime, h.Time())
		}
		if val := node2.Recv(h).Message; val != "hi node 2 (message 2)" {
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
	for _, node := range []*Node{node1, node2} {
		loop.Go(func(h *Handle) {
			h.Poll(node.Incoming)
		})
		if loop.Run() == nil {
			t.Error("expected deadlock error")
		}
	}
}
