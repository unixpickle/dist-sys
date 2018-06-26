package allreduce

import (
	"github.com/unixpickle/dist-sys/collcomm"
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
func (s StreamAllreducer) Allreduce(c *collcomm.Comms, data []float64,
	fn collcomm.ReduceFn) []float64 {
	if len(data) == 0 || len(c.Ports) == 1 {
		return data
	}
	if c.Index() == 0 {
		return s.allreduceRoot(c, data)
	}
	return s.allreduceOther(c, data, fn)
}

func (s StreamAllreducer) allreduceRoot(c *collcomm.Comms, data []float64) []float64 {
	chunksOut := s.chunkify(c, data)
	reduced := make([]float64, 0, len(data))

	// Kick off the reduction cycle.
	(&streamPacket{packetType: streamPacketReduce, payload: chunksOut[0]}).Send(c)
	chunksOut = chunksOut[1:]

	// Push the reduction through the ring.
	waitingReduceAck := true
	for len(reduced) < len(data) {
		packet := recvStreamPacket(c)
		switch packet.packetType {
		case streamPacketReduce:
			reduced = append(reduced, packet.payload...)
			(&streamPacket{packetType: streamPacketReduceAck}).Send(c)
		case streamPacketReduceAck:
			if !waitingReduceAck {
				panic("unexpected ACK")
			}
			if len(chunksOut) > 0 {
				(&streamPacket{packetType: streamPacketReduce, payload: chunksOut[0]}).Send(c)
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
	for _, chunk := range s.chunkify(c, reduced) {
		(&streamPacket{packetType: streamPacketBcast, payload: chunk}).Send(c)
		for {
			packet := recvStreamPacket(c)
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

func (s StreamAllreducer) allreduceOther(c *collcomm.Comms, data []float64, fn collcomm.ReduceFn) []float64 {
	var reduced []float64

	isLastNode := c.Index()+1 == len(c.Ports)

	// Reduce our data into the stream.
	var reduceBlocked bool
	var reduceBuf []*streamPacket
	remainingData := data
	for len(reduced) == 0 {
		packet := recvStreamPacket(c)
		switch packet.packetType {
		case streamPacketReduce:
			(&streamPacket{packetType: streamPacketReduceAck}).Send(c)
			chunk := fn(c.Handle, packet.payload, remainingData[:len(packet.payload)])
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
			(&streamPacket{packetType: streamPacketBcastAck}).Send(c)
			if !isLastNode {
				// Otherwise, the packet will never reach
				// the next node in the ring.
				packet.Send(c)
			}
		default:
			panic("unexpected packet type")
		}
		if !reduceBlocked && len(reduceBuf) > 0 {
			reduceBuf[0].Send(c)
			essentials.OrderedDelete(&reduceBuf, 0)
			reduceBlocked = true
		}
	}

	// Read the broadcasted reduction.
	bcastBlocked := true
	var bcastBuf []*streamPacket
	for len(reduced) < len(data) || len(bcastBuf) > 0 {
		packet := recvStreamPacket(c)
		switch packet.packetType {
		case streamPacketReduceAck:
			if !reduceBlocked {
				panic("unexpected ACK")
			}
			reduceBlocked = false
		case streamPacketBcast:
			reduced = append(reduced, packet.payload...)
			(&streamPacket{packetType: streamPacketBcastAck}).Send(c)
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
			bcastBuf[0].Send(c)
			essentials.OrderedDelete(&bcastBuf, 0)
			bcastBlocked = true
		}
	}

	if reduceBlocked {
		panic("missed expected ACK")
	}

	return reduced
}

func (s StreamAllreducer) chunkify(c *collcomm.Comms, data []float64) [][]float64 {
	granularity := s.Granularity
	if granularity == 0 {
		granularity = 1
	}
	chunkSize := len(data) / (len(c.Ports) * granularity)
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

func recvStreamPacket(c *collcomm.Comms) *streamPacket {
	msg := c.Port.Recv(c.Handle)
	return msg.Message.(*streamPacket)
}

func (s *streamPacket) Size() float64 {
	return float64(len(s.payload)*8) + 1.0
}

// Send sends the packet to the appropriate host.
// For ACKs, this is the previous host.
// For other messages, this is the next host.
func (s *streamPacket) Send(c *collcomm.Comms) {
	idx := c.Index()
	var dstIdx int
	if s.packetType == streamPacketReduceAck || s.packetType == streamPacketBcastAck {
		dstIdx = idx - 1
		if dstIdx < 0 {
			dstIdx = len(c.Ports) - 1
		}
	} else {
		dstIdx = (idx + 1) % len(c.Ports)
	}
	c.Network.Send(c.Handle, &simulator.Message{
		Source:  c.Port,
		Dest:    c.Ports[dstIdx],
		Message: s,
		Size:    s.Size(),
	})
}

type streamChunkInfo struct {
	data  []float64
	start int
}
