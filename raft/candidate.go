package raft

import (
	"context"

	"github.com/unixpickle/dist-sys/simulator"
)

type Candidate[C Command, S StateMachine[C, S]] struct {
	Context context.Context
	Handle  *simulator.Handle
	Network simulator.Network
	Port    *simulator.Port
	Others  []*simulator.Port

	// Algorithm state.
	Log  *Log[C, S]
	Term int64

	// ElectionTimeout should be randomized per follower to
	// break ties in the common case.
	ElectionTimeout float64

	// Internal state
	timerStream *simulator.EventStream
	timer       *simulator.Timer
}

// RunLoop waits until an election is complete.
//
// Returns nil if this node is the follower.
// Returns a message from another node if this node is a
// follower, and this message should be handled by the
// follower loop.
func (c *Candidate[C, S]) RunLoop() *simulator.Message {
	c.timerStream = c.Handle.Stream()
	c.timer = c.Handle.Schedule(c.timerStream, nil, c.ElectionTimeout)

	defer func() {
		c.Handle.Cancel(c.timer)
	}()

	numVotes := 0
	for {
		result := c.Handle.Poll(c.timerStream, c.Port.Incoming)
		select {
		case <-c.Context.Done():
			return nil
		default:
		}
		if result.Stream == c.timerStream {
			c.Term++
			c.timer = c.Handle.Schedule(c.timerStream, nil, c.ElectionTimeout)
			c.broadcastCandidacy()
			continue
		}

		rawMsg := result.Message.(*simulator.Message)

		if msg, ok := rawMsg.Message.(*RaftMessage[C, S]); ok {
			if term := msg.Term(); term < c.Term {
				continue
			} else if term > c.Term {
				return rawMsg
			} else if msg.VoteResponse != nil {
				if msg.VoteResponse.ReceivedVote {
					numVotes += 1
				}
				if numVotes >= len(c.Others)/2 {
					return nil
				}
			} else if msg.AppendLogs != nil {
				return rawMsg
			}
		}
	}
}

func (c *Candidate[C, S]) broadcastCandidacy() {
	var messages []*simulator.Message
	for _, port := range c.Others {
		latestTerm, latestIndex := c.Log.LatestTermAndIndex()
		raftMsg := &RaftMessage[C, S]{
			Vote: &Vote{
				Term:        c.Term,
				LatestTerm:  latestTerm,
				LatestIndex: latestIndex,
			},
		}
		messages = append(messages, &simulator.Message{
			Source:  c.Port,
			Dest:    port,
			Message: raftMsg,
			Size:    float64(raftMsg.Size()),
		})
	}
	c.Network.Send(c.Handle, messages...)
}
