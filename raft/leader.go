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
	callbacks  map[int64]*simulator.Port
	commandIDs map[int64]string
}

// RunLoop runs the leader loop until we stop being the
// leader, at which point the first non-leader message is
// returned.
func (l *Leader[C, S]) RunLoop() *simulator.Message {
	l.followerLogIndices = make([]int64, len(l.Followers))
	for i := range l.followerLogIndices {
		l.followerLogIndices[i] = l.Log.OriginIndex
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
	followerIndex := sourcePortIndex(rawMessage, l.Followers)
	if followerIndex == -1 {
		command := rawMessage.Message.(*CommandMessage[C])
		l.handleCommand(rawMessage.Source, command)
		return true
	}

	msg := rawMessage.Message.(*RaftMessage[C, S])
	if t := msg.Term(); t < l.Term {
		return true
	} else if t > l.Term {
		return false
	}

	resp := msg.AppendLogsResponse

	if resp == nil {
		// This could be a residual vote from this term after
		// we had enough votes to become leader.
		return true
	}

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

func (l *Leader[C, S]) handleCommand(port *simulator.Port, command *CommandMessage[C]) {
	idx := l.Log.Append(l.Term, command.Command)
	l.callbacks[idx] = port
	l.commandIDs[idx] = command.ID
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
		CommitIndex: l.Log.OriginIndex,
	}}
	if logIndex < l.Log.OriginIndex {
		// We must include the entire state machine.
		msg.AppendLogs.OriginIndex = l.Log.OriginIndex
		msg.AppendLogs.OriginTerm = l.Log.OriginTerm
		originState := l.Log.Origin.Clone()
		msg.AppendLogs.Origin = &originState
		msg.AppendLogs.Entries = append([]LogEntry[C]{}, l.Log.Entries...)
	} else {
		msg.AppendLogs.OriginIndex = logIndex
		msg.AppendLogs.OriginTerm = msg.AppendLogs.Entries[logIndex-l.Log.OriginIndex].Term
		msg.AppendLogs.Entries = append(
			[]LogEntry[C]{},
			msg.AppendLogs.Entries[logIndex-l.Log.OriginIndex:]...,
		)
	}
	return msg
}

func (l *Leader[C, S]) maybeAdvanceCommit() {
	sorted := append([]int64{}, l.followerLogIndices...)
	sort.Slice(sorted, func(i int, j int) bool {
		return sorted[i] < sorted[j]
	})
	half := len(sorted) / 2
	minCommit := sorted[half]

	if minCommit > l.Log.OriginIndex {
		commitEntry := l.Log.Entries[minCommit-l.Log.OriginIndex]
		if commitEntry.Term != l.Term {
			// We can only safely commit log entries from the
			// current term, and previous entries will be
			// implicitly committed as a result.
			return
		}

		oldCommit := l.Log.OriginIndex
		results := l.Log.Commit(minCommit)

		// Now that we have committed, we can send results to clients.
		var callbackMessages []*simulator.Message
		for i, result := range results {
			index := int64(i) + oldCommit
			if cb, ok := l.callbacks[index]; ok {
				resp := &CommandResponse{Result: result, ID: l.commandIDs[index]}
				callbackMessages = append(callbackMessages, &simulator.Message{
					Source:  l.Port,
					Dest:    cb,
					Message: resp,
					Size:    float64(resp.Size()),
				})
				delete(l.callbacks, index)
				delete(l.commandIDs, index)
			}
		}
		l.Network.Send(l.Handle, callbackMessages...)

		l.sendAppendLogsAndResetTimer()
	}
}

func sourcePortIndex(msg *simulator.Message, ports []*simulator.Port) int {
	for i, f := range ports {
		if msg.Source == f {
			return i
		}
	}
	return -1
}
