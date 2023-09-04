package raft

import (
	"sort"

	"github.com/unixpickle/dist-sys/simulator"
)

type Leader[C Command, S StateMachine[C, S]] struct {
	// Network configuration
	Handle    *simulator.Handle
	Network   simulator.Network
	Port      *simulator.Port
	Followers []*simulator.Port

	// Algorithm state.
	Log  *Log[C, S]
	Term int64

	// Settings
	HeartbeatInterval float64

	// followerLogIndices stores the latest of our log indices
	// propagated to each follower. Starts at latest commit, and
	// may be reduced or increased based on responses.
	followerLogIndices []int64

	// Used for triggering AppendLogs heartbeats.
	timer       *simulator.Timer
	timerStream *simulator.EventStream

	// Maps log indices to client connections for sending state
	// machine results back to clients.
	callbacks map[int64]*simulator.Port
}

// RunLoop runs the leader loop until we stop being the
// leader, at which point the first non-leader message is
// returned.
func (l *Leader[C, S]) RunLoop() *simulator.Message {
	l.followerLogIndices = make([]int64, len(l.Followers))
	for i := range l.followerLogIndices {
		l.followerLogIndices[i] = l.Log.CommitIndex
	}
	l.timerStream = l.Handle.Stream()
	l.timer = l.Handle.Schedule(l.timerStream, nil, l.HeartbeatInterval)
	defer func() {
		// Timer will be updated every time it fires.
		l.Handle.Cancel(l.timer)
	}()

	for {
		l.sendAppendLogs()

		event := l.Handle.Poll(l.timerStream, l.Port.Incoming)
		if event.Stream == l.timerStream {
			l.timer = l.Handle.Schedule(l.timerStream, nil, l.HeartbeatInterval)
			l.sendAppendLogs()
		} else {
			msg := event.Message.(*simulator.Message)
			if !l.handleMessage(msg) {
				return msg
			}
		}
	}
}

func (l *Leader[C, S]) handleMessage(rawMessage *simulator.Message) bool {
	followerIndex := -1
	for i, f := range l.Followers {
		if rawMessage.Source == f {
			followerIndex = i
			break
		}
	}
	if followerIndex == -1 {
		// This is from a client.
		command := rawMessage.Message.(C)
		l.handleCommand(rawMessage.Source, command)
	}

	msg := rawMessage.Message.(*RaftMessage[C, S])
	if t := msg.Term(); t < l.Term {
		return true
	} else if t > l.Term {
		return false
	}

	resp := msg.AppendLogsResponse
	if resp.Success {
		l.followerLogIndices[followerIndex] = resp.LatestIndex
		l.maybeAdvanceCommit()
	} else {
		l.followerLogIndices[followerIndex] = resp.CommitIndex
		msg := l.appendLogsForFollower(followerIndex)
		rawMsg := &simulator.Message{
			Source:  l.Port,
			Dest:    l.Followers[followerIndex],
			Message: msg,
			Size:    float64(msg.Size()),
		}
		l.Network.Send(l.Handle, rawMsg)
	}

	return true
}

func (l *Leader[C, S]) handleCommand(port *simulator.Port, command C) {
	idx := l.Log.Append(l.Term, command)
	l.callbacks[idx] = port
	l.sendAppendLogsAndResetTimer()
}

func (l *Leader[C, S]) sendAppendLogsAndResetTimer() {
	l.Handle.Cancel(l.timer)
	l.sendAppendLogs()
	l.timer = l.Handle.Schedule(l.timerStream, nil, l.HeartbeatInterval)
}

func (l *Leader[C, S]) sendAppendLogs() {
	messages := make([]*simulator.Message, len(l.Followers))
	for i, port := range l.Followers {
		msg := l.appendLogsForFollower(i)
		messages = append(messages, &simulator.Message{
			Source:  l.Port,
			Dest:    port,
			Message: msg,
			Size:    float64(msg.Size()),
		})
	}
	l.Network.Send(l.Handle, messages...)
}

func (l *Leader[C, S]) appendLogsForFollower(i int) *RaftMessage[C, S] {
	logIndex := l.followerLogIndices[i]
	msg := &RaftMessage[C, S]{AppendLogs: &AppendLogs[C, S]{
		Term:        l.Term,
		CommitIndex: l.Log.CommitIndex,
	}}
	if logIndex < l.Log.OriginIndex {
		// We must include the entire state machine.
		msg.AppendLogs.OriginIndex = l.Log.OriginIndex
		originState := l.Log.Origin.Clone()
		msg.AppendLogs.Origin = &originState
		msg.AppendLogs.Entries = append([]LogEntry[C]{}, l.Log.Entries...)
	} else {
		msg.AppendLogs.OriginIndex = logIndex
		msg.AppendLogs.Entries = append(
			[]LogEntry[C]{},
			msg.AppendLogs.Entries[int(logIndex-l.Log.OriginIndex):]...,
		)
	}
	return msg
}

func (l *Leader[C, S]) maybeAdvanceCommit() {
	if term, _ := l.Log.LatestTermAndIndex(); term != l.Term {
		// We can only safely commit if the latest log entry is
		// from this term, otherwise this could be overwritten by
		// another future leader.
		return
	}

	sorted := append([]int64{}, l.followerLogIndices...)
	sort.Slice(sorted, func(i int, j int) bool {
		return sorted[i] < sorted[j]
	})
	half := len(sorted) / 2
	minCommit := sorted[half]

	if minCommit > l.Log.CommitIndex {
		oldCommit := l.Log.CommitIndex
		results := l.Log.Commit(minCommit)

		// Now that we have committed, we can send results to clients.
		var callbackMessages []*simulator.Message
		for i, result := range results {
			index := int64(i) + oldCommit
			if cb, ok := l.callbacks[index]; ok {
				callbackMessages = append(callbackMessages, &simulator.Message{
					Source:  l.Port,
					Dest:    cb,
					Message: result,
					Size:    float64(result.Size()),
				})
				delete(l.callbacks, index)
			}
		}
		l.Network.Send(l.Handle, callbackMessages...)

		l.sendAppendLogsAndResetTimer()
	}
}
