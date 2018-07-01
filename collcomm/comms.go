package collcomm

import "github.com/unixpickle/dist-sys/simulator"

// Comms manages a set of connections between a bunch of
// nodes.
// During a collective operation, each node has a local
// Comms object that represents its view of the world.
// A new Comms object should be used for each operation,
// thus automatically handling multiplexing.
type Comms struct {
	// Handle is the node's main Goroutine's handle on the
	// event loop.
	Handle *simulator.Handle

	// Port is the current node's port.
	Port *simulator.Port

	// Ports contains ports to all the nodes in the
	// network, including the current node.
	Ports []*simulator.Port

	// Network is the network connecting the nodes.
	Network simulator.Network
}

// SpawnComms creates Comms objects for every node in a
// network and calls f for each node in its own Goroutine.
func SpawnComms(loop *simulator.EventLoop, network simulator.Network, nodes []*simulator.Node,
	f func(c *Comms)) {
	ports := make([]*simulator.Port, len(nodes))
	for i, node := range nodes {
		ports[i] = node.Port(loop)
	}
	for i := range nodes {
		port := ports[i]
		loop.Go(func(h *simulator.Handle) {
			f(&Comms{
				Handle:  h,
				Port:    port,
				Ports:   ports,
				Network: network,
			})
		})
	}
}

// Size gets the number of nodes.
func (c *Comms) Size() int {
	return len(c.Ports)
}

// Bcast sends a vector to every other node.
func (c *Comms) Bcast(vec []float64) {
	messages := make([]*simulator.Message, 0, len(c.Ports)-1)
	for _, port := range c.Ports {
		if port == c.Port {
			continue
		}
		messages = append(messages, &simulator.Message{
			Source:  c.Port,
			Dest:    port,
			Message: vec,
			Size:    float64(len(vec) * 8),
		})
	}
	c.Network.Send(c.Handle, messages...)
}

// Send schedules a message to be sent to the destination.
func (c *Comms) Send(dst *simulator.Port, vec []float64) {
	c.Network.Send(c.Handle, &simulator.Message{
		Source:  c.Port,
		Dest:    dst,
		Message: vec,
		Size:    float64(len(vec) * 8),
	})
}

// Recv receives the next vector.
func (c *Comms) Recv() ([]float64, *simulator.Port) {
	res := c.Port.Recv(c.Handle)
	return res.Message.([]float64), res.Source
}

// Index returns the current node's index in the list of
// nodes.
func (c *Comms) Index() int {
	return c.IndexOf(c.Port)
}

// IndexOf returns any node's index.
func (c *Comms) IndexOf(p *simulator.Port) int {
	for i, port := range c.Ports {
		if port == p {
			return i
		}
	}
	panic("unreachable")
}
