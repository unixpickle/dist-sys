package raft

import (
	"context"

	"github.com/unixpickle/dist-sys/simulator"
)

type Follower[C Command, S StateMachine[C, S]] struct {
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

	currentVote *simulator.Port
	leader      *simulator.Port
}

// RunLoop runs as a follower until there is a timeout, at
// which point the node should enter the candidate phase.
func (f *Follower[C, S]) RunLoop(initialMessage *simulator.Message) {
	if initialMessage != nil {
		f.handleMessage(initialMessage)
	}

	f.timerStream = f.Handle.Stream()
	f.timer = f.Handle.Schedule(f.timerStream, nil, f.ElectionTimeout)

	for {
		result := f.Handle.Poll(f.timerStream, f.Port.Incoming)
		select {
		case <-f.Context.Done():
			return
		default:
		}
		if result.Stream == f.timerStream {
			return
		}
		f.handleMessage(result.Message.(*simulator.Message))
	}
}

func (f *Follower[C, S]) handleMessage(rawMsg *simulator.Message) {
	if sourcePortIndex(rawMsg, f.Others) == -1 {
		f.handleCommand(rawMsg.Source, rawMsg.Message.(*CommandMessage[C]))
		return
	}

	msg := rawMsg.Message.(*RaftMessage[C, S])
	if term := msg.Term(); term < f.Term {
		return
	} else if term > f.Term {
		f.currentVote = nil
		f.leader = nil
		f.Term = term
	}

	if msg.AppendLogs != nil {
		f.handleAppendLogs(rawMsg.Source, msg.AppendLogs)
	} else if msg.Vote != nil {
		f.handleVote(rawMsg.Source, msg.Vote)
	}
}

func (f *Follower[C, S]) resetTimer() {
	f.Handle.Cancel(f.timer)
	f.timer = f.Handle.Schedule(f.timerStream, nil, f.ElectionTimeout)
}

func (f *Follower[C, S]) handleCommand(source *simulator.Port, msg *CommandMessage[C]) {
	var leader *simulator.Node
	if f.leader != nil {
		leader = f.leader.Node
	}
	resp := &CommandResponse{
		ID:       msg.ID,
		Redirect: leader,
	}
	f.Network.Send(f.Handle, &simulator.Message{
		Source:  f.Port,
		Dest:    source,
		Message: resp,
		Size:    float64(resp.Size()),
	})
}

func (f *Follower[C, S]) handleAppendLogs(source *simulator.Port, msg *AppendLogs[C, S]) {
	f.resetTimer()
	f.leader = source

	lastIndex, lastTerm := f.Log.LatestTermAndIndex()
	newIndex := msg.OriginIndex + int64(len(msg.Entries))
	if lastTerm == msg.Term && lastIndex > newIndex {
		// Don't allow later log messages to be overwritten
		// by out-of-order messages from the leader.
		return
	}

	if msg.Origin != nil {
		// This is the easy case: they are forcing our log to be
		// in a particular state.
		f.Log.OriginIndex = msg.OriginIndex
		f.Log.OriginTerm = msg.OriginTerm
		f.Log.Origin = *msg.Origin
		f.Log.Entries = msg.Entries

		_, latestIndex := f.Log.LatestTermAndIndex()
		resp := &RaftMessage[C, S]{
			AppendLogsResponse: &AppendLogsResponse[C, S]{
				Term:        f.Term,
				SeqNum:      msg.SeqNum,
				CommitIndex: msg.CommitIndex,
				LatestIndex: latestIndex,
				Success:     true,
			},
		}
		f.Network.Send(f.Handle, &simulator.Message{
			Source:  f.Port,
			Dest:    source,
			Message: resp,
			Size:    float64(resp.Size()),
		})
		return
	}

	resp := &RaftMessage[C, S]{
		AppendLogsResponse: &AppendLogsResponse[C, S]{
			Term:    f.Term,
			SeqNum:  msg.SeqNum,
			Success: false,
		},
	}

	if msg.OriginIndex <= f.Log.OriginIndex {
		// We have already committed zero or more of these entries,
		// so the rest must be accepted.
		if int(f.Log.OriginIndex-msg.OriginIndex) <= len(msg.Entries) {
			f.Log.Entries = msg.Entries[f.Log.OriginIndex-msg.OriginIndex:]
			f.Log.Commit(msg.CommitIndex)
		} else {
			// The sender actually truncated our logs, meaning
			// they do not have a commit as far as we do.
			panic("this should not happen")
			// f.Log.Entries = []LogEntry[C]{}
		}
		resp.AppendLogsResponse.Success = true
	} else {
		startEntry := msg.OriginIndex - f.Log.OriginIndex
		// We want to make sure they aren't appending past our log
		if int(startEntry) <= len(f.Log.Entries) {
			// We don't want to accept this suffix if we weren't consistent
			// up to the prefix.
			if f.Log.Entries[startEntry-1].Term == msg.OriginTerm {
				// We can append all the entries after this.
				if len(msg.Entries) > 0 {
					// We only append and reallocate if this isn't a heartbeat.
					f.Log.Entries = append(
						append(
							[]LogEntry[C]{},
							f.Log.Entries[:startEntry]...,
						),
						msg.Entries...,
					)
				}
				f.Log.Commit(msg.CommitIndex)
				resp.AppendLogsResponse.Success = true
			}
		}
	}

	resp.AppendLogsResponse.CommitIndex = f.Log.OriginIndex
	_, resp.AppendLogsResponse.LatestIndex = f.Log.LatestTermAndIndex()

	f.Network.Send(f.Handle, &simulator.Message{
		Source:  f.Port,
		Dest:    source,
		Message: resp,
		Size:    float64(resp.Size()),
	})
}

func (f *Follower[C, S]) handleVote(source *simulator.Port, msg *Vote) {
	latestTerm, latestIndex := f.Log.LatestTermAndIndex()
	resp := &RaftMessage[C, S]{
		VoteResponse: &VoteResponse{
			Term:         f.Term,
			ReceivedVote: true,
		},
	}
	if (f.currentVote != nil && source != f.currentVote) ||
		latestTerm > msg.LatestTerm ||
		(latestTerm == msg.LatestTerm && latestIndex > msg.LatestIndex) {
		resp.VoteResponse.ReceivedVote = false
	} else {
		f.currentVote = source
	}
	f.Network.Send(f.Handle, &simulator.Message{
		Source:  f.Port,
		Dest:    source,
		Message: resp,
		Size:    float64(resp.Size()),
	})
}
