package simulator

import (
	"fmt"
	"testing"
	"time"
)

func ExampleEventLoop() {
	loop := NewEventLoop()
	stream := loop.Stream()
	loop.Go(func(h *Handle) {
		msg := h.Poll(stream).Message
		fmt.Println(msg, h.Time())
	})
	loop.Go(func(h *Handle) {
		message := "Hello, world!"
		delay := 15.5
		h.Schedule(stream, message, delay)
	})
	loop.Run()
	// Output: Hello, world! 15.5
}

func TestEventLoopTimer(t *testing.T) {
	loop := NewEventLoop()
	stream := loop.Stream()
	value := make(chan interface{}, 1)
	loop.Go(func(h *Handle) {
		value <- h.Poll(stream).Message
	})
	loop.Go(func(h *Handle) {
		h.Schedule(stream, 1337, 15.5)
	})
	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}
	if loop.Time() != 15.5 {
		t.Errorf("time should be 15.5 but is %f", loop.Time())
	}
	select {
	case val := <-value:
		if val != 1337 {
			t.Errorf("value should be 1337 but is %f", val)
		}
	default:
		t.Error("timer never fired")
	}
}

func TestEventLoopTimerOrder(t *testing.T) {
	loop := NewEventLoop()

	stream1 := loop.Stream()
	stream2 := loop.Stream()

	values := make(chan interface{}, 2)

	for _, stream := range []*EventStream{stream1, stream2} {
		s := stream
		loop.Go(func(h *Handle) {
			event := h.Poll(s)
			if event.Stream != s {
				t.Error("incorrect stream")
			}
			values <- event.Message
		})
	}

	loop.Go(func(h *Handle) {
		h.Schedule(stream1, 123, 5.0)
		h.Schedule(stream2, 1339, 7.0)
	})

	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	if loop.Time() != 7.0 {
		t.Errorf("time should be 7.0 but got %f", loop.Time())
	}

	val1 := <-values
	val2 := <-values
	if val1 != 123 {
		t.Errorf("value 1 should be 123 but got %d", val1)
	}
	if val2 != 1339 {
		t.Errorf("value 2 should be 1339 but got %d", val2)
	}
}

// TestEventLoopMultiConsumer tests that the EventLoop
// properly supports multiple threads reading from the
// same event stream.
func TestEventLoopMultiConsumer(t *testing.T) {
	orderings := map[[3]int]bool{}
	for i := 0; i < 10000; i++ {
		loop := NewEventLoop()
		stream := loop.Stream()
		var ordering [3]int
		for j := 0; j < 3; j++ {
			idx := j
			loop.Go(func(h *Handle) {
				msg := h.Poll(stream).Message
				ordering[idx] = msg.(int)
			})
		}
		loop.Go(func(h *Handle) {
			h.Schedule(stream, 1, 1.0)
			h.Schedule(stream, 2, 2.0)
			h.Schedule(stream, 3, 3.0)
		})
		if err := loop.Run(); err != nil {
			t.Fatal(err)
		}
		if loop.Time() != 3 {
			t.Errorf("time should be 3.0 but got %f", loop.Time())
		}
		orderings[ordering] = true
	}
	if len(orderings) != 6 {
		t.Errorf("expected 6 possible orderings but saw %d", len(orderings))
	}
}

// TestEventLoopBuffering tests that messages sent to an
// EventStream will be queued if no Goroutine is currently
// polling on the stream.
func TestEventLoopBuffering(t *testing.T) {
	loop := NewEventLoop()

	readFirst := loop.Stream()
	readSecond := loop.Stream()
	neverRead := loop.Stream()

	value := make(chan interface{}, 1)

	loop.Go(func(h *Handle) {
		h.Poll(readFirst)
		value <- h.Poll(readSecond).Message
	})

	loop.Go(func(h *Handle) {
		h.Schedule(readSecond, 1337, 3.0)
		h.Sleep(2)
		h.Schedule(neverRead, 321, 4.0)
		h.Schedule(readFirst, 123, 7.0)
	})

	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	if loop.Time() != 9.0 {
		t.Errorf("time should be 9.0 but got %f", loop.Time())
	}

	if val := <-value; val != 1337 {
		t.Errorf("expected 1337 but got %d", val)
	}
}

// TestEventLoopPollMulti tests polling multiple streams
// at once.
func TestEventLoopPollMulti(t *testing.T) {
	loop := NewEventLoop()

	first := loop.Stream()
	second := loop.Stream()
	third := loop.Stream()

	values := make(chan interface{}, 3)

	loop.Go(func(h *Handle) {
		for _, stream := range []*EventStream{first, second, third} {
			event := h.Poll(third, second, first)
			if event.Stream != stream {
				t.Error("incorrect stream order")
			}
			values <- event.Message
		}
	})

	loop.Go(func(h *Handle) {
		h.Schedule(first, 133, 3.0)
		h.Sleep(3.5)
		h.Schedule(third, 333, 7.0)

		// Real time should play no part in the ordering
		// of messages.
		time.Sleep(time.Second / 4)

		h.Schedule(second, 233, 1.0)
	})

	if err := loop.Run(); err != nil {
		t.Fatal(err)
	}

	if loop.Time() != 10.5 {
		t.Errorf("time should be 10.5 but got %f", loop.Time())
	}

	for _, expected := range []int{133, 233, 333} {
		if val := <-values; val != expected {
			t.Errorf("expected %d but got %d", expected, val)
		}
	}
}

// TestEventLoopDeadlocks makes sure that the event loop
// can detect deadlocks.
func TestEventLoopDeadlocks(t *testing.T) {
	loop := NewEventLoop()

	stream1 := loop.Stream()
	stream2 := loop.Stream()

	loop.Go(func(h *Handle) {
		h.Poll(stream1)
		h.Schedule(stream2, 1337, 0.0)
	})

	loop.Go(func(h *Handle) {
		time.Sleep(time.Second / 4)
		h.Poll(stream2)
		h.Schedule(stream1, 1337, 0.0)
	})

	if loop.Run() == nil {
		t.Error("did not detect deadlock")
	}
}
