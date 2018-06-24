# dist-sys

This repository contains toy implementations of various distributed algorithms. The goal is to teach myself about distributed systems without actually using a ton of machines.

# Resources

 * Allreduce algorithms: [Optimization of Collective Communication Operations in MPICH](http://www.mcs.anl.gov/~thakur/papers/ijhpca-coll.pdf)

# Packages

## simulator

This package provides an API for simulating a distributed network of machines. It has two core APIs:

 * An event loop, which schedules events in "virtual" time.
 * A simulated network, which sits on top of the event loop to provide realistic message delivery times.

To use the event loop, first create an `EventLoop` with `NewEventLoop()`. Then use `loop.Go()` to run new Goroutines, each of which gets its own `Handle` to the event loop. To wait for events, use `handle.Poll()`. Virtual time will only pass while all Goroutines with `Handle`s are polling on those `Handle`s. This way, Goroutines can do any amount of real-world work without virtual time passing, and vice versa. If you want virtual time to reflect some time for computation, you can use `Handle.Sleep()` to explicitly let a certain amount of virtual time pass.

This event loop example shows communication between two Goroutines:

```go
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
```

The network API sits on top of the event loop API. Here's an example of how one might use the network API to time a simple back-and-forth interaction between nodes:

```go
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
```
