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

	HeartbeatInterval float64

	followerLogIndices []int64
	timer              *simulator.Timer
	timerStream        *simulator.EventStream
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
		panic("unknown source for message")
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

	// TODO: send callbacks to succeeded client calls here
	if minCommit > l.Log.CommitIndex {
		l.Log.Commit(minCommit)
	}

	l.Handle.Cancel(l.timer)
	l.sendAppendLogs()
	l.timer = l.Handle.Schedule(l.timerStream, nil, l.HeartbeatInterval)
}
