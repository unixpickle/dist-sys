package allreduce

import "github.com/unixpickle/dist-sys/simulator"

// Host stores information about the entire network from a
// single node's perspective.
type Host struct {
	// Handle is the nodes' main Goroutine's handle on the
	// event loop.
	Handle *simulator.Handle

	// Node is the current node.
	Node *simulator.Node

	// Nodes contains all the nodes in the network,
	// including Node.
	Nodes []*simulator.Node

	// Network is the network connecting the nodes.
	Network simulator.Network
}

// Bcast sends a vector to every other node.
func (h *Host) Bcast(vec []float64) {
	messages := make([]*simulator.Message, 0, len(h.Nodes)-1)
	for _, node := range h.Nodes {
		if node == h.Node {
			continue
		}
		messages = append(messages, &simulator.Message{
			Source:  h.Node,
			Dest:    node,
			Message: vec,
			Size:    float64(len(vec) * 8),
		})
	}
	h.Network.Send(h.Handle, messages...)
}

// Send schedules a message to be sent to the destination.
func (h *Host) Send(dst *simulator.Node, vec []float64) {
	h.Network.Send(h.Handle, &simulator.Message{
		Source:  h.Node,
		Dest:    dst,
		Message: vec,
		Size:    float64(len(vec) * 8),
	})
}

// Recv receives the next vector.
func (h *Host) Recv() ([]float64, *simulator.Node) {
	res := h.Node.Recv(h.Handle)
	return res.Message.([]float64), res.Source
}
