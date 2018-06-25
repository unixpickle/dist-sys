# dist-sys

The main goal of this repository is to play with distributed systems *without* needing a large cluster of machines. As I learn about distributed algorithms, I'm going to implement them here.

# Resources

 * Allreduce algorithms: [Optimization of Collective Communication Operations in MPICH](http://www.mcs.anl.gov/~thakur/papers/ijhpca-coll.pdf)

# Packages

 * [simulator](#simulator) - simulate a network of machines using "virtual" time.
 * [allreduce](#allreduce) - quickly sum or max large vectors across many machines.

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

## allreduce

This package contains some Allreduce implementations. For those who are not familiar, allreduce is extremely useful in large-scale machine learning. It allows you to quickly sum vectors (gradients, in most cases) across a large cluster of machines.

I've implemented three algorithms for allreduce:

 * Naive: every node sends its vector to every other node.
 * Tree: the nodes arrange themselves into a binary tree, and reduce up and down the tree.
 * Stream: the nodes arrange themselves into a ring, and the vector streams around the ring twice. The first time is for reduction, and the second time is to broadcast the reduced vector to the other nodes.

Here are some performance results for the different algorithms in a simulated network. The last row is the most realistic for a datacenter.

| Nodes | Latency | NIC rate | Size | Naive | Tree | Stream |
|:--|:--|:--|:--|:--|:--|:--|
| 2 | 0.1 | 1E+06 | 10 | 0.100080 | 0.200160 | 0.800232 |
| 2 | 0.1 | 1E+06 | 10000 | 0.180020 | 0.360030 | 1.020026 |
| 2 | 0.1 | 1E+06 | 10000000 | 80.120000 | 160.230000 | 200.860011 |
| 16 | 0.001 | 1E+06 | 10 | 0.002200 | 0.008880 | 0.067676 |
| 16 | 0.001 | 1E+06 | 10000 | 1.201160 | 1.047350 | 0.419556 |
| 16 | 0.001 | 1E+06 | 10000000 | 1200.161000 | 1040.107250 | 305.143356 |
| 32 | 0.1 | 1E+06 | 10 | 0.102480 | 1.001120 | 9.901000 |
| 32 | 0.1 | 1E+06 | 10000 | 2.580320 | 2.272630 | 19.551941 |
| 32 | 0.1 | 1E+06 | 10000000 | 2480.420000 | 1361.042500 | 336.256147 |
| 32 | 0.1 | 1E+09 | 10 | 0.100003 | 1.000001 | 9.900001 |
| 32 | 0.1 | 1E+09 | 10000 | 0.102800 | 1.001270 | 19.100486 |
| 32 | 0.1 | 1E+09 | 10000000 | 2.900000 | 2.402500 | 19.185915 |
| 32 | 0.0001 | 1E+09 | 10 | 0.000103 | 0.001001 | 0.009901 |
| 32 | 0.0001 | 1E+09 | 10000 | 0.002900 | 0.002403 | 0.019586 |
| 32 | 0.0001 | 1E+09 | 10000000 | 2.800100 | 1.490913 | 0.358567 |
