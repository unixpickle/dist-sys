package allreduce

import (
	"github.com/unixpickle/dist-sys/simulator"
	"github.com/unixpickle/essentials"
)

// A StreamAllreducer splits a vector up into smaller
// messages and streams the messages through all the nodes
// at once.
//
// The reduction has two phases: Reduce and Broadcast.
// During Reduce, the fully reduced vector arrives at the
// first node.
// During Broadcast, the reduced vector is streamed from
// the first node to all the other nodes.
type StreamAllreducer struct {
	// Granularity determines how many chunks the data is
	// split up into.
	// The actual number of chunks is multiplied by the
	// number of nodes.
	//
	// If Granularity is 0, it is treated as 1.
	Granularity int
}

// Allreduce calls fn on chunks of data at a time and
// returns a vector resulting from the final reduction.
func (s StreamAllreducer) Allreduce(h *Host, data []float64, fn ReduceFn) []float64 {
	if len(data) == 0 || len(h.Nodes) == 1 {
		return data
	}
	if h.Index() == 0 {
		return s.allreduceRoot(h, data)
	}
	return s.allreduceOther(h, data, fn)
}

func (s StreamAllreducer) allreduceRoot(h *Host, data []float64) []float64 {
	chunksOut := s.chunkify(h, data)
	reduced := make([]float64, 0, len(data))

	// Kick off the reduction cycle.
	(&streamPacket{packetType: streamPacketReduce, payload: chunksOut[0]}).Send(h)
	chunksOut = chunksOut[1:]

	// Push the reduction through the ring.
	waitingReduceAck := true
	for len(reduced) < len(data) {
		packet := recvStreamPacket(h)
		switch packet.packetType {
		case streamPacketReduce:
			reduced = append(reduced, packet.payload...)
			(&streamPacket{packetType: streamPacketReduceAck}).Send(h)
		case streamPacketReduceAck:
			if !waitingReduceAck {
				panic("unexpected ACK")
			}
			if len(chunksOut) > 0 {
				(&streamPacket{packetType: streamPacketReduce, payload: chunksOut[0]}).Send(h)
				chunksOut = chunksOut[1:]
			} else {
				waitingReduceAck = false
			}
		default:
			panic("unexpected packet type")
		}
	}

	if len(chunksOut) > 0 {
		panic("unexpected reduction completion")
	} else if len(reduced) != len(data) {
		panic("excess data")
	}

	// Push the data through the bcast cycle.
	for _, chunk := range s.chunkify(h, reduced) {
		(&streamPacket{packetType: streamPacketBcast, payload: chunk}).Send(h)
		for {
			packet := recvStreamPacket(h)
			if packet.packetType == streamPacketReduceAck {
				if !waitingReduceAck {
					panic("unexpected ACK")
				}
				waitingReduceAck = false
			} else if packet.packetType == streamPacketBcastAck {
				break
			} else {
				panic("unexpected packet type")
			}
		}
	}

	return reduced
}

func (s StreamAllreducer) allreduceOther(h *Host, data []float64, fn ReduceFn) []float64 {
	var reduced []float64

	isLastNode := h.Index()+1 == len(h.Nodes)

	// Reduce our data into the stream.
	var reduceBlocked bool
	var reduceBuf []*streamPacket
	remainingData := data
	for len(reduced) == 0 {
		packet := recvStreamPacket(h)
		switch packet.packetType {
		case streamPacketReduce:
			(&streamPacket{packetType: streamPacketReduceAck}).Send(h)
			chunk := fn(h.Handle, packet.payload, remainingData[:len(packet.payload)])
			remainingData = remainingData[len(packet.payload):]
			outPacket := &streamPacket{packetType: streamPacketReduce, payload: chunk}
			reduceBuf = append(reduceBuf, outPacket)
		case streamPacketReduceAck:
			if !reduceBlocked {
				panic("unexpected ACK")
			}
			reduceBlocked = false
		case streamPacketBcast:
			if len(reduceBuf) > 0 {
				panic("got bcast before reduce finished")
			}
			reduced = append(reduced, packet.payload...)
			(&streamPacket{packetType: streamPacketBcastAck}).Send(h)
			if !isLastNode {
				// Otherwise, the packet will never reach
				// the next node in the ring.
				packet.Send(h)
			}
		default:
			panic("unexpected packet type")
		}
		if !reduceBlocked && len(reduceBuf) > 0 {
			reduceBuf[0].Send(h)
			essentials.OrderedDelete(&reduceBuf, 0)
			reduceBlocked = true
		}
	}

	// Read the broadcasted reduction.
	bcastBlocked := true
	var bcastBuf []*streamPacket
	for len(reduced) < len(data) || len(bcastBuf) > 0 {
		packet := recvStreamPacket(h)
		switch packet.packetType {
		case streamPacketReduceAck:
			if !reduceBlocked {
				panic("unexpected ACK")
			}
			reduceBlocked = false
		case streamPacketBcast:
			reduced = append(reduced, packet.payload...)
			(&streamPacket{packetType: streamPacketBcastAck}).Send(h)
			if !isLastNode {
				outPacket := &streamPacket{packetType: streamPacketBcast, payload: packet.payload}
				bcastBuf = append(bcastBuf, outPacket)
			}
		case streamPacketBcastAck:
			if !bcastBlocked {
				panic("unexpected ACK")
			}
			bcastBlocked = false
		default:
			panic("unexpected packet type")
		}
		if !bcastBlocked && len(bcastBuf) > 0 {
			bcastBuf[0].Send(h)
			essentials.OrderedDelete(&bcastBuf, 0)
			bcastBlocked = true
		}
	}

	if reduceBlocked {
		panic("missed expected ACK")
	}

	return reduced
}

func (s StreamAllreducer) chunkify(h *Host, data []float64) [][]float64 {
	granularity := s.Granularity
	if granularity == 0 {
		granularity = 1
	}
	chunkSize := len(data) / (len(h.Nodes) * granularity)
	if chunkSize < 1 {
		chunkSize = 1
	}
	var res [][]float64
	for i := 0; i < len(data); i += chunkSize {
		if i+chunkSize > len(data) {
			res = append(res, data[i:])
		} else {
			res = append(res, data[i:i+chunkSize])
		}
	}
	return res
}

type streamPacketType int

const (
	streamPacketReduce streamPacketType = iota
	streamPacketReduceAck
	streamPacketBcast
	streamPacketBcastAck
)

type streamPacket struct {
	packetType streamPacketType
	payload    []float64
}

func recvStreamPacket(h *Host) *streamPacket {
	msg := h.Node.Recv(h.Handle)
	return msg.Message.(*streamPacket)
}

func (s *streamPacket) Size() float64 {
	return float64(len(s.payload)*8) + 1.0
}

// Send sends the packet to the appropriate host.
// For ACKs, this is the previous host.
// For other messages, this is the next host.
func (s *streamPacket) Send(h *Host) {
	idx := h.Index()
	var dstIdx int
	if s.packetType == streamPacketReduceAck || s.packetType == streamPacketBcastAck {
		dstIdx = idx - 1
		if dstIdx < 0 {
			dstIdx = len(h.Nodes) - 1
		}
	} else {
		dstIdx = (idx + 1) % len(h.Nodes)
	}
	h.Network.Send(h.Handle, &simulator.Message{
		Source:  h.Node,
		Dest:    h.Nodes[dstIdx],
		Message: s,
		Size:    s.Size(),
	})
}

type streamChunkInfo struct {
	data  []float64
	start int
}
