package raft

import (
	"github.com/unixpickle/dist-sys/simulator"
)

type Leader[L LogEntry, S StateMachine[L, S]] struct {
	// Network configuration
	Handle    *simulator.Handle
	Network   simulator.Network
	Port      *simulator.Port
	Followers []*simulator.Port

	// Algorithm state.
	Log  *Log[L, S]
	Term int64

	HeartbeatInterval float64

	followerLogIndices []int64
}

// RunLoop runs the leader loop until we stop being the
// leader, at which point the first non-leader message is
// returned.
func (l *Leader[L, S]) RunLoop() *simulator.Message {
	l.followerLogIndices = make([]int64, len(l.Followers))
	for i := range l.followerLogIndices {
		l.followerLogIndices[i] = l.Log.CommitIndex
	}
	timerStream := l.Handle.Stream()
	timer := l.Handle.Schedule(timerStream, nil, l.HeartbeatInterval)
	defer func() {
		// Timer will be updated every time it fires.
		l.Handle.Cancel(timer)
	}()

	for {
		l.sendAppendLogs()

		event := l.Handle.Poll(timerStream, l.Port.Incoming)
		if event.Stream == timerStream {
			timer = l.Handle.Schedule(timerStream, nil, l.HeartbeatInterval)
			l.sendAppendLogs()
		} else {
			msg := event.Message.(*simulator.Message)
			if !l.handleMessage(msg) {
				return msg
			}
		}
	}
}

func (l *Leader[L, S]) handleMessage(rawMessage *simulator.Message) bool {
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

	// Figure out what's going on.
	msg := rawMessage.Message.(*RaftMessage[L, S])
	if msg.Term() != l.Term {
		return false
	}

	// TODO: handle appendlogs response here.
	return true
}

func (l *Leader[L, S]) sendAppendLogs() {
	messages := make([]*simulator.Message, len(l.Followers))
	for i, logIndex := range l.followerLogIndices {
		msg := &RaftMessage[L, S]{AppendLogs: &AppendLogs[L, S]{
			Term:        l.Term,
			CommitIndex: l.Log.CommitIndex,
		}}
		if logIndex < l.Log.OriginIndex {
			// We must include the entire state machine.
			msg.AppendLogs.OriginIndex = l.Log.OriginIndex
			originState := l.Log.Origin.Clone()
			msg.AppendLogs.Origin = &originState
			msg.AppendLogs.Entries = append([]L{}, l.Log.Entries...)
		} else {
			msg.AppendLogs.OriginIndex = logIndex
			msg.AppendLogs.Entries = append(
				[]L{},
				msg.AppendLogs.Entries[int(logIndex-l.Log.OriginIndex):]...,
			)
		}
		messages = append(messages, &simulator.Message{
			Source:  l.Port,
			Dest:    l.Followers[i],
			Message: msg,
			Size:    float64(msg.Size()),
		})
	}
	l.Network.Send(l.Handle, messages...)
}
