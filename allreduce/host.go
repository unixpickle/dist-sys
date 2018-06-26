package allreduce

import "github.com/unixpickle/dist-sys/simulator"

// Host stores information about the entire network from a
// single node's perspective.
type Host struct {
	// Handle is the nodes' main Goroutine's handle on the
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

// Bcast sends a vector to every other node.
func (h *Host) Bcast(vec []float64) {
	messages := make([]*simulator.Message, 0, len(h.Ports)-1)
	for _, port := range h.Ports {
		if port == h.Port {
			continue
		}
		messages = append(messages, &simulator.Message{
			Source:  h.Port,
			Dest:    port,
			Message: vec,
			Size:    float64(len(vec) * 8),
		})
	}
	h.Network.Send(h.Handle, messages...)
}

// Send schedules a message to be sent to the destination.
func (h *Host) Send(dst *simulator.Port, vec []float64) {
	h.Network.Send(h.Handle, &simulator.Message{
		Source:  h.Port,
		Dest:    dst,
		Message: vec,
		Size:    float64(len(vec) * 8),
	})
}

// Recv receives the next vector.
func (h *Host) Recv() ([]float64, *simulator.Port) {
	res := h.Port.Recv(h.Handle)
	return res.Message.([]float64), res.Source
}

// Index returns the current node's index in the list of
// nodes.
func (h *Host) Index() int {
	return h.IndexOf(h.Port)
}

// IndexOf returns any node's index.
func (h *Host) IndexOf(p *simulator.Port) int {
	for i, port := range h.Ports {
		if port == p {
			return i
		}
	}
	panic("unreachable")
}
