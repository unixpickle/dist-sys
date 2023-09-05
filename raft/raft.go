package raft

import (
	"context"

	"github.com/unixpickle/dist-sys/simulator"
)

type Raft[C Command, S StateMachine[C, S]] struct {
	Context context.Context
	Handle  *simulator.Handle
	Network simulator.Network
	Port    *simulator.Port
	Others  []*simulator.Port

	// Algorithm state.
	Log  *Log[C, S]
	Term int64

	// Settings for timeouts
	ElectionTimeout   float64
	HeartbeatInterval float64
}

func (r *Raft[C, S]) RunLoop() {
	var followerMsg *simulator.Message
	for {
		f := &Follower[C, S]{
			Context: r.Context,
			Handle:  r.Handle,
			Network: r.Network,
			Port:    r.Port,
			Others:  r.Others,
			Log:     r.Log,
			Term:    r.Term,

			ElectionTimeout: r.ElectionTimeout,
		}
		f.RunLoop(followerMsg)
		select {
		case <-r.Context.Done():
			return
		default:
		}
		r.Term = f.Term + 1

		c := &Candidate[C, S]{
			Context: r.Context,
			Handle:  r.Handle,
			Network: r.Network,
			Port:    r.Port,
			Others:  r.Others,
			Log:     r.Log,
			Term:    r.Term,

			ElectionTimeout: r.ElectionTimeout,
		}
		followerMsg = c.RunLoop()
		select {
		case <-r.Context.Done():
			return
		default:
		}
		if followerMsg == nil {
			followerMsg = (&Leader[C, S]{
				Context:   r.Context,
				Handle:    r.Handle,
				Network:   r.Network,
				Port:      r.Port,
				Followers: r.Others,
				Log:       r.Log,
				Term:      r.Term,

				HeartbeatInterval: r.HeartbeatInterval,
			}).RunLoop()
			select {
			case <-r.Context.Done():
				return
			default:
			}
		}
	}
}
