package simulator

import (
	"math"
	"math/rand"
	"sync"

	"github.com/unixpickle/essentials"
)

// A Node represents a machine on a virtual network.
type Node struct {
	unused int
}

// NewNode creates a new, unique Node.
func NewNode() *Node {
	return &Node{}
}

// Port creates a new Port connected to the Node.
func (n *Node) Port(loop *EventLoop) *Port {
	return &Port{Node: n, Incoming: loop.Stream()}
}

// A Port identifies a point of communication on a Node.
// Data is sent from Ports and received on Ports.
type Port struct {
	// The Node to which the Port is attached.
	Node *Node

	// A stream of *Message objects.
	Incoming *EventStream
}

// Recv receives the next message.
func (p *Port) Recv(h *Handle) *Message {
	return h.Poll(p.Incoming).Message.(*Message)
}

// A Message is a chunk of data sent between nodes over a
// network.
type Message struct {
	Source  *Port
	Dest    *Port
	Message interface{}
	Size    float64
}

// A Network represents an abstract way of communicating
// between nodes.
type Network interface {
	// Send message objects from one node to another.
	// The message will arrive on the receiving port's
	// incoming EventStream if the communication is
	// successful.
	//
	// This is a non-blocking operation.
	//
	// It is preferrable to pass multiple messages in at
	// once, if possible.
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
		src, dst := nodeToIndex[msg.msg.Source.Node], nodeToIndex[msg.msg.Dest.Node]
		mat.Set(src, dst, 1)
		counts.Set(src, dst, counts.Get(src, dst)+1)
	}
	s.switcher.SwitchedRates(mat)
	for _, msg := range state {
		src, dst := nodeToIndex[msg.msg.Source.Node], nodeToIndex[msg.msg.Dest.Node]
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

// An OrderedNetwork delivers messages sent to endpoints in
// order, while allowing non-determinism and temporarily
// disconnected nodes.
type OrderedNetwork struct {
	Rate             float64
	MaxRandomLatency float64

	lock      sync.Mutex
	nextTimes map[*Node]float64
	downNodes map[*Node]bool
	timers    map[*Node][]*Timer
}

func NewOrderedNetwork(rate float64, maxRandomLatency float64) *OrderedNetwork {
	return &OrderedNetwork{
		Rate:             rate,
		MaxRandomLatency: maxRandomLatency,
		nextTimes:        map[*Node]float64{},
		downNodes:        map[*Node]bool{},
		timers:           map[*Node][]*Timer{},
	}
}

// Send sends the messages over the network in order.
func (o *OrderedNetwork) Send(h *Handle, msgs ...*Message) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.cleanupTimers(h)

	curTime := h.Time()

	for _, msg := range msgs {
		src := msg.Source.Node
		dest := msg.Dest.Node
		if o.downNodes[src] || o.downNodes[dest] {
			continue
		}
		latency := rand.Float64() * o.MaxRandomLatency
		delay := latency + msg.Size/o.Rate

		var timer *Timer
		if t, ok := o.nextTimes[dest]; !ok || t <= curTime {
			timer = h.Schedule(msg.Dest.Incoming, msg, delay)
			o.nextTimes[dest] = curTime + delay
		} else {
			timer = h.Schedule(msg.Dest.Incoming, msg, delay+(t-curTime))
			o.nextTimes[dest] = delay + t
		}
		o.timers[dest] = append(o.timers[dest], timer)
		o.timers[src] = append(o.timers[src], timer)
	}
}

func (o *OrderedNetwork) cleanupTimers(h *Handle) {
	time := h.Time()
	o.filterTimer(h, func(t *Timer) bool {
		return t.Time() >= time
	})
}

func (o *OrderedNetwork) SetDown(h *Handle, node *Node, down bool) {
	o.lock.Lock()
	defer o.lock.Unlock()

	o.downNodes[node] = down

	if !down {
		return
	}

	delete(o.nextTimes, node)

	// Kill all active messages to and from the node.
	o.cleanupTimers(h)
	timers := o.timers[node]
	canceled := map[*Timer]bool{}
	for _, t := range timers {
		canceled[t] = true
		h.Cancel(t)
	}
	delete(o.timers, node)
	o.filterTimer(h, func(t *Timer) bool {
		return !canceled[t]
	})
}

func (o *OrderedNetwork) filterTimer(h *Handle, f func(t *Timer) bool) {
	var keys []*Node
	for k := range o.timers {
		keys = append(keys, k)
	}
	for _, k := range keys {
		timers := o.timers[k]
		for i := 0; i < len(timers); i++ {
			if !f(timers[i]) {
				essentials.UnorderedDelete(&timers, i)
				i--
			}
		}
		o.timers[k] = timers
	}
}
