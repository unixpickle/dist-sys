package simulator

import (
	"math"
	"math/rand"
	"sync"
)

// A Message is a chunk of data sent between nodes over a
// network.
type Message struct {
	Source  *Node
	Dest    *Node
	Message interface{}
	Size    float64
}

// A Node represents a single machine in a network.
type Node struct {
	// Incoming is a stream of *Message objects.
	Incoming *EventStream
}

// Recv receives the next message.
func (n *Node) Recv(h *Handle) *Message {
	return h.Poll(n.Incoming).Message.(*Message)
}

// A Network represents an abstract way of communicating
// between nodes.
type Network interface {
	// Send message objects from one node to another.
	// The message will arrive on the receiving node's
	// incoming EventStream if the communication is
	// successful.
	//
	// This is a non-blocking operation.
	//
	// It is preferrable to pass multiple messages in at
	// once if possible.
	// Otherwise, the Network may have to continually
	// re-plan the entire message delivery timeline.
	Send(h *Handle, msgs ...*Message)
}

// A RandomNetwork is a network that assigns random delays
// to every message.
type RandomNetwork struct{}

// Send sends the messages with random delays.
func (r RandomNetwork) Send(h *Handle, msgs ...*Message) {
	for _, msg := range msgs {
		h.Schedule(msg.Dest.Incoming, msg, rand.Float64())
	}
}

// A SwitcherNetwork is a network where data is passed
// through a Switcher. Multiple messages along the same
// edge are sent concurrently, potentially making each one
// take longer to arrive at its destination.
type SwitcherNetwork struct {
	lock sync.Mutex

	switcher Switcher
	nodes    []*Node
	latency  float64

	plan switchedPlan
}

// NewSwitcherNetwork creates a new SwitcherNetwork.
//
// The latency argument adds an extra constant-length
// timeout to every message delivery.
// The latency period does influence oversubscription,
// so one message's latency period may interfere with
// another message's transmission.
// In practice, this may result in twice the latency-based
// congestion that would actually occur in a network.
func NewSwitcherNetwork(switcher Switcher, nodes []*Node, latency float64) *SwitcherNetwork {
	return &SwitcherNetwork{
		switcher: switcher,
		nodes:    nodes,
		latency:  latency,
	}
}

// Send sends the message over the network.
//
// This may affect the speed of messages that are already
// being transmitted.
func (s *SwitcherNetwork) Send(h *Handle, msgs ...*Message) {
	s.lock.Lock()
	defer s.lock.Unlock()

	state := s.stopPlan(h)
	for _, msg := range msgs {
		state = append(state, &switchedMsg{
			msg:              msg,
			remainingLatency: s.latency,
			remainingSize:    msg.Size,
		})
	}
	s.createPlan(h, state)
}

func (s *SwitcherNetwork) stopPlan(h *Handle) []*switchedMsg {
	var currentState []*switchedMsg
	for _, step := range s.plan {
		if h.Time() >= step.endTime {
			// The timers may have fired, so we let this go.
			continue
		}
		if h.Time() >= step.startTime {
			// Interpolate in the current segment.
			elapsed := h.Time() - step.startTime
			for _, msg := range step.startState {
				currentState = append(currentState, msg.AddTime(elapsed))
			}
		}
		for _, timer := range step.timers {
			h.Cancel(timer)
		}
	}
	return currentState
}

func (s *SwitcherNetwork) computeDataRates(state []*switchedMsg) {
	nodeToIndex := map[*Node]int{}
	for i, node := range s.nodes {
		nodeToIndex[node] = i
	}

	// Technically this is a tiny bit incorrect, since the
	// latency period isn't taken into account.
	// Really, during the latency period, the sender NIC
	// is clogged up but the receiver NIC is not.

	mat := NewConnMat(len(s.nodes))
	counts := NewConnMat(len(s.nodes))
	for _, msg := range state {
		src, dst := nodeToIndex[msg.msg.Source], nodeToIndex[msg.msg.Dest]
		mat.Set(src, dst, 1)
		counts.Set(src, dst, counts.Get(src, dst)+1)
	}
	s.switcher.SwitchedRates(mat)
	for _, msg := range state {
		src, dst := nodeToIndex[msg.msg.Source], nodeToIndex[msg.msg.Dest]
		msg.dataRate = mat.Get(src, dst) / counts.Get(src, dst)
	}
}

func (s *SwitcherNetwork) createPlan(h *Handle, state []*switchedMsg) {
	s.plan = make(switchedPlan, 0, len(state))
	startTime := h.Time()
	for len(state) > 0 {
		s.computeDataRates(state)

		nextMsgs, newState, lowestETA := messagesWithLowestETA(state)

		timers := make([]*Timer, len(nextMsgs))
		for i, msg := range nextMsgs {
			delay := startTime - h.Time() + lowestETA
			timers[i] = h.Schedule(msg.msg.Dest.Incoming, msg.msg, delay)
		}

		endTime := timers[0].Time()
		s.plan = append(s.plan, &switchedPlanSegment{
			startTime:  startTime,
			endTime:    endTime,
			timers:     timers,
			startState: state,
		})

		for i, msg := range newState {
			newState[i] = msg.AddTime(endTime - startTime)
		}
		state = newState
		startTime = endTime
	}
}

// switchedMsg encodes the state of a message that is
// being sent through the network.
type switchedMsg struct {
	msg *Message

	remainingLatency float64

	remainingSize float64
	dataRate      float64
}

// ETA gets the time until the message is sent.
func (s *switchedMsg) ETA() float64 {
	return math.Max(0, s.remainingLatency+s.remainingSize/s.dataRate)
}

// AddTime updates the message's state to reflect a
// certain amount of time elapsing.
func (s *switchedMsg) AddTime(t float64) *switchedMsg {
	res := *s

	if t < res.remainingLatency {
		res.remainingLatency -= t
		return &res
	}

	t -= res.remainingLatency
	res.remainingLatency = 0
	res.remainingSize -= res.dataRate * t

	return &res
}

// switchedPlanSegment represents a period of time during
// which the message state is not changing, aside from
// more data being sent or more latency being paid for.
//
// Each segment ends with at least one Timer, which
// notifies a node about a received message.
type switchedPlanSegment struct {
	startTime float64
	endTime   float64
	timers    []*Timer

	startState []*switchedMsg
}

// switchedPlan represents a sequence of switched state
// changes that, together, send all of the current
// messages on the network.
type switchedPlan []*switchedPlanSegment

func messagesWithLowestETA(msgs []*switchedMsg) (lowest, rest []*switchedMsg, lowestETA float64) {
	etas := make([]float64, len(msgs))
	for i, msg := range msgs {
		etas[i] = msg.ETA()
	}
	lowestETA = etas[0]
	for _, eta := range etas {
		if eta < lowestETA {
			lowestETA = eta
		}
	}

	lowest = make([]*switchedMsg, 0, 1)
	rest = make([]*switchedMsg, 0, len(msgs)-1)

	for i, msg := range msgs {
		if etas[i] == lowestETA {
			lowest = append(lowest, msg)
		} else {
			rest = append(rest, msg)
		}
	}

	return lowest, rest, lowestETA
}
