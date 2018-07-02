package simulator

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/unixpickle/essentials"
)

// An EventStream is a uni-directional channel of events
// that are passed through an EventLoop.
//
// It is only safe to use an EventStream on one EventLoop
// at once.
type EventStream struct {
	loop    *EventLoop
	pending []interface{}
}

// An Event is a message received on some EventStream.
type Event struct {
	Message interface{}
	Stream  *EventStream
}

// A Timer controls the delayed delivery of an event.
// In particular, a Timer represents a single send that
// will happen in the (virtual) future.
type Timer struct {
	time  float64
	event *Event
}

// Time gets the time when the timer will be fired.
//
// If the virtual time is lower than a timer's Time(),
// then it is guaranteed that the timer has not fired.
func (t *Timer) Time() float64 {
	return t.time
}

// A Handle is a Goroutine's mechanism for accessing an
// EventLoop. Goroutines should not share Handles.
type Handle struct {
	*EventLoop

	// These fields are empty when the Goroutine is
	// not polling on any streams.
	pollStreams []*EventStream
	pollChan    chan<- *Event
}

// Poll waits for the next event from a set of streams.
func (h *Handle) Poll(streams ...*EventStream) *Event {
	ch := make(chan *Event, 1)
	h.modifyHandles(func() {
		if h.pollStreams != nil {
			panic("Handle is shared between Goroutines")
		}
		for _, stream := range streams {
			if len(stream.pending) > 0 {
				msg := stream.pending[0]
				essentials.OrderedDelete(&stream.pending, 0)
				ch <- &Event{Message: msg, Stream: stream}
				return
			}
		}
		h.pollStreams = streams
		h.pollChan = ch
	})
	return <-ch
}

// Schedule creates a Timer for delivering an event.
func (h *Handle) Schedule(stream *EventStream, msg interface{}, delay float64) *Timer {
	if stream.loop != h.EventLoop {
		panic("EventStream is not associated with the correct EventLoop")
	}
	var timer *Timer
	h.modify(func() {
		timer = &Timer{
			time:  h.time + delay,
			event: &Event{Message: msg, Stream: stream},
		}
		if math.IsInf(timer.time, 0) || math.IsNaN(timer.time) {
			panic(fmt.Sprintf("invalid deadline: %f", timer.time))
		}
		h.timers = append(h.timers, timer)
	})
	return timer
}

// Cancel stops a timer if the timer is scheduled.
//
// If the timer is not scheduled, this has no effect.
func (h *Handle) Cancel(t *Timer) {
	h.modify(func() {
		for i, timer := range h.timers {
			if timer == t {
				essentials.UnorderedDelete(&h.timers, i)
			}
		}
	})
}

// Sleep waits for a certain amount of virtual time to
// elapse.
func (h *Handle) Sleep(delay float64) {
	stream := h.Stream()
	h.Schedule(stream, nil, delay)
	h.Poll(stream)
}

// An EventLoop is a global scheduler for events in a
// simulated distributed system.
//
// All Goroutines which access an EventLoop should be
// started using the EventLoop.Go() method.
//
// The event loop will only run when all active Goroutines
// are polling for an event.
// This way, simulated machines don't have to worry about
// real timing while performing computations.
type EventLoop struct {
	lock    sync.Mutex
	timers  []*Timer
	handles []*Handle

	time float64

	running  bool
	notifyCh chan struct{}
}

// NewEventLoop creates an event loop.
//
// The event loop's clock starts at 0.
func NewEventLoop() *EventLoop {
	return &EventLoop{notifyCh: make(chan struct{}, 1)}
}

// Stream creates a new EventStream.
func (e *EventLoop) Stream() *EventStream {
	return &EventStream{loop: e}
}

// Go runs a function in a Goroutine and passes it a new
// handle to the EventLoop.
func (e *EventLoop) Go(f func(h *Handle)) {
	h := &Handle{EventLoop: e}
	e.lock.Lock()
	e.handles = append(e.handles, h)
	e.lock.Unlock()
	go func() {
		f(h)
		e.modifyHandles(func() {
			for i, handle := range e.handles {
				if handle == h {
					essentials.UnorderedDelete(&e.handles, i)
					return
				}
			}
			panic("cannot free handle that does not exist")
		})
	}()
}

// Run runs the loop and blocks until all handles have
// been closed.
//
// It is not safe to run the loop from more than one
// Goroutine at once.
//
// Returns with an error if there is a deadlock.
func (e *EventLoop) Run() error {
	e.lock.Lock()
	if e.running {
		e.lock.Unlock()
		panic("EventLoop is already running.")
	}
	e.running = true
	e.lock.Unlock()

	defer func() {
		e.lock.Lock()
		e.running = false
		e.lock.Unlock()
	}()

	for _ = range e.notifyCh {
		if shouldContinue, err := e.step(); !shouldContinue {
			return err
		}
	}

	panic("unreachable")
}

// MustRun is like Run, but it panics if there is a
// deadlock.
func (e *EventLoop) MustRun() {
	if err := e.Run(); err != nil {
		panic(err)
	}
}

// Time gets the current virtual time.
func (e *EventLoop) Time() float64 {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.time
}

// modify calls a function f() such that f can safely
// change the loop state.
//
// This assumes that handle states are not being modified,
// meaning that no scheduling changes can occur.
// If this is not the case, use modifyHandles.
func (e *EventLoop) modify(f func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	f()
}

// modifyHandles is like modify(), but it may alter the
// loop state in such a way that scheduling changes occur.
func (e *EventLoop) modifyHandles(f func()) {
	e.lock.Lock()
	defer func() {
		e.lock.Unlock()
		select {
		case e.notifyCh <- struct{}{}:
		default:
		}
	}()
	f()
}

// step runs the next event on the loop, if possible.
//
// If the event loop can no longer run, the first return
// value is false.
// If this is due to an error, the second argument
// indicates the error.
func (e *EventLoop) step() (bool, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if len(e.handles) == 0 {
		return false, nil
	}

	for _, h := range e.handles {
		if len(h.pollStreams) == 0 {
			// Do not run the loop while a Goroutine is
			// doing work in real-time.
			return true, nil
		}
	}

	for len(e.timers) > 0 {
		// Shuffle so that two timers with the same deadline
		// don't execute in a deterministic order.
		indices := rand.Perm(len(e.timers))

		minTimerIdx := indices[0]
		for _, i := range indices[1:] {
			if e.timers[i].time < e.timers[minTimerIdx].time {
				minTimerIdx = i
			}
		}
		timer := e.timers[minTimerIdx]

		essentials.UnorderedDelete(&e.timers, minTimerIdx)
		e.time = math.Max(e.time, timer.time)
		if e.deliver(timer.event) {
			return true, nil
		}
	}

	return false, errors.New("deadlock: all Handles are polling")
}

func (e *EventLoop) deliver(event *Event) bool {
	// Shuffle the handles so that two receivers don't get
	// messages in a deterministic order.
	indices := rand.Perm(len(e.handles))
	for _, i := range indices {
		h := e.handles[i]
		for _, stream := range h.pollStreams {
			if stream == event.Stream {
				h.pollChan <- event
				h.pollChan = nil
				h.pollStreams = nil
				return true
			}
		}
	}
	event.Stream.pending = append(event.Stream.pending, event.Message)
	return false
}
