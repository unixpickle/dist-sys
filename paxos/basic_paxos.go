package paxos

import (
	"github.com/unixpickle/dist-sys/simulator"
	"github.com/unixpickle/essentials"
)

// BasicPaxos implements the simplest version of the Paxos
// algorithm.
//
// This algorithm does not support re-configuration,
// multiple values, more than N/2 failures, etc.
type BasicPaxos struct {
	// NoBackoff controls how a proposer deals with
	// conflict.
	// If set, proposers will not use any exponential
	// backoff process.
	NoBackoff bool

	// Timeout is the number of seconds to wait before
	// giving up on a message.
	Timeout float64
}

// Propose runs a Paxos proposer until a value has been
// accepted by a quorum of acceptors.
//
// The cluster must contain at least 3 acceptors.
func (b *BasicPaxos) Propose(h *simulator.Handle, n simulator.Network, p *simulator.Port,
	acceptors []*simulator.Port, value interface{}, size int) interface{} {
	quorum := quorumSize(len(acceptors))
	sendVal := &basicValue{value: value, size: size}
	var round int
	for i := 0; true; i++ {
		// TODO: exponential backoff here.

		prepare := &basicPrepareReq{round: round}
		for _, acc := range acceptors {
			n.Send(h, &simulator.Message{
				Source:  p,
				Dest:    acc,
				Size:    float64(prepare.Size()),
				Message: prepare,
			})
		}

		acceptorQuorum, prepResps := b.prepareResponses(h, p, acceptors, round)
		if len(acceptorQuorum) < quorum {
			round = essentials.MaxInt(round, basicPrepareNextRound(prepResps))
			continue
		}

		if v := basicPrepareAcceptedValue(prepResps); v != nil {
			sendVal = v
		}

		accept := &basicAcceptReq{round: round, value: sendVal}
		for _, acc := range acceptorQuorum {
			n.Send(h, &simulator.Message{
				Source:  p,
				Dest:    acc,
				Size:    float64(accept.Size()),
				Message: accept,
			})
		}

		if b.proposeResponse(h, p, len(acceptorQuorum), quorum, round) {
			return sendVal.value
		}

		round++
	}
	panic("unreachable")
}

// Accept runs a Paxos acceptor.
//
// Returns when an event is sent to the done stream.
func (b *BasicPaxos) Accept(h *simulator.Handle, n simulator.Network, p *simulator.Port,
	done *simulator.EventStream) {
	currentRound := -1
	var acceptedRound int
	var acceptedValue *basicValue
	for {
		event := h.Poll(done, p.Incoming)
		if event.Stream == done {
			return
		}
		msg := event.Message.(*simulator.Message)
		switch packet := msg.Message.(type) {
		case *basicPrepareReq:
			response := &basicPrepareResp{}
			if packet.round > currentRound {
				currentRound = packet.round
				response.success = true
				response.acceptedRound = acceptedRound
				response.acceptedValue = acceptedValue
			}
			response.currentRound = currentRound
			n.Send(h, &simulator.Message{
				Source:  msg.Dest,
				Dest:    msg.Source,
				Size:    float64(response.Size()),
				Message: response,
			})
		case *basicAcceptReq:
			response := &basicAcceptResp{}
			response.round = packet.round
			if packet.round == currentRound {
				acceptedRound = packet.round
				acceptedValue = packet.value
				response.accepted = true
			}
			n.Send(h, &simulator.Message{
				Source:  msg.Dest,
				Dest:    msg.Source,
				Size:    float64(response.Size()),
				Message: response,
			})
		}
	}
}

// prepareResponses reads responses from a prepare step.
//
// It returns the ports for all acceptors that accepted
// the round number, and all the responses.
func (b *BasicPaxos) prepareResponses(h *simulator.Handle, p *simulator.Port,
	acceptors []*simulator.Port, round int) ([]*simulator.Port, []*basicPrepareResp) {
	timeout := h.Stream()
	h.Schedule(timeout, nil, b.Timeout)

	var accPorts []*simulator.Port
	var responses []*basicPrepareResp
	for len(responses) < len(acceptors) {
		msg := h.Poll(timeout, p.Incoming)
		if msg.Stream == timeout {
			break
		}
		netMsg := msg.Message.(*simulator.Message)
		if resp, ok := netMsg.Message.(*basicPrepareResp); ok {
			if resp.currentRound == round {
				responses = append(responses, resp)
				if resp.success {
					accPorts = append(accPorts, netMsg.Source)
				}
			}
		}
	}
	return accPorts, responses
}

// proposeResponse reads responses to an accept request
// and determines whether the value was accepted or not.
func (b *BasicPaxos) proposeResponse(h *simulator.Handle, p *simulator.Port,
	numSent, quorum, round int) bool {
	timeout := h.Stream()
	h.Schedule(timeout, nil, b.Timeout)

	var numAccepts int
	var numAcceptResponses int
	for numAcceptResponses < numSent && numAccepts < quorum {
		msg := h.Poll(timeout, p.Incoming)
		if msg.Stream == timeout {
			break
		}
		netMsg := msg.Message.(*simulator.Message)
		if resp, ok := netMsg.Message.(*basicAcceptResp); ok {
			if resp.round == round {
				numAcceptResponses++
				if resp.accepted {
					numAccepts++
				}
			}
		}
	}

	return numAccepts >= quorum
}

type basicValue struct {
	value interface{}
	size  int
}

func (b *basicValue) Size() int {
	if b == nil {
		return 0
	}
	return b.size
}

type basicPrepareReq struct {
	round int
}

func (b *basicPrepareReq) Size() int {
	return 8
}

type basicPrepareResp struct {
	success bool

	// On success, this is the promised round number.
	// Otherwise, this is the round number that superseded
	// the requested round number.
	currentRound int

	// Non-nil if a previous value was accepted.
	acceptedValue *basicValue
	acceptedRound int
}

func basicPrepareAcceptedValue(resps []*basicPrepareResp) *basicValue {
	highest := -1
	var value *basicValue
	for _, r := range resps {
		if r.acceptedRound > highest {
			highest = r.acceptedRound
			value = r.acceptedValue
		}
	}
	return value
}

func basicPrepareNextRound(resps []*basicPrepareResp) int {
	var max int
	for _, r := range resps {
		if r.currentRound > max {
			max = r.currentRound
		}
	}
	return max + 1
}

func (b *basicPrepareResp) Size() int {
	return 17 + b.acceptedValue.Size()
}

type basicAcceptReq struct {
	round int
	value *basicValue
}

func (b *basicAcceptReq) Size() int {
	return 8 + b.value.Size()
}

type basicAcceptResp struct {
	round    int
	accepted bool
}

func (b *basicAcceptResp) Size() int {
	return 9
}

func quorumSize(numNodes int) int {
	return numNodes/2 + 1
}
