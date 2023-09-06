package raft

import (
	"context"
	"math"
	"sort"

	"github.com/unixpickle/dist-sys/simulator"
)

type Leader[C Command, S StateMachine[C, S]] struct {
	Context context.Context

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

	// followerKnownLogIndices stores the latest of our log
	// indices confirmed by each follower. Starts at latest
	// commit, and may be reduced or increased based on
	// responses.
	followerKnownLogIndices []int64

	// followerSentLogIndices is like the above field, but
	// may be higher if we have sent some log entries that
	// have not been acknowledged.
	followerSentLogIndices []int64

	// Used to track when to send new AppendLogs and
	// determine if a response is stale.
	seqNum       int64
	lastSeqNums  []int64
	lastSentTime []float64

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
	l.callbacks = map[int64]*simulator.Port{}
	l.commandIDs = map[int64]string{}
	l.followerKnownLogIndices = make([]int64, len(l.Followers))
	l.followerSentLogIndices = make([]int64, len(l.Followers))
	l.lastSeqNums = make([]int64, len(l.Followers))
	l.lastSentTime = make([]float64, len(l.Followers))
	l.seqNum = 1
	for i := 0; i < len(l.Followers); i++ {
		l.followerKnownLogIndices[i] = l.Log.OriginIndex
		l.followerSentLogIndices[i] = l.Log.OriginIndex
		l.lastSentTime[i] = math.Inf(-1)
	}
	l.timerStream = l.Handle.Stream()
	l.timer = l.Handle.Schedule(l.timerStream, nil, l.HeartbeatInterval/2)
	defer func() {
		// Timer will be updated every time it fires.
		l.Handle.Cancel(l.timer)
	}()

	l.sendAppendLogs()

	for {
		event := l.Handle.Poll(l.timerStream, l.Port.Incoming)
		select {
		case <-l.Context.Done():
			return nil
		default:
		}
		if event.Stream == l.timerStream {
			l.timer = l.Handle.Schedule(l.timerStream, nil, l.HeartbeatInterval/2)
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

	if resp.SeqNum != l.lastSeqNums[followerIndex] {
		// This is a stale response.
		return true
	}

	// Make note that no message is in flight so that we are
	// now free to send more messages.
	l.lastSeqNums[followerIndex] = 0

	if resp.Success {
		l.followerKnownLogIndices[followerIndex] = resp.LatestIndex
		l.maybeAdvanceCommit()

		if l.lastSeqNums[followerIndex] != 0 ||
			l.followerSentLogIndices[followerIndex] == l.Log.OriginIndex+int64(len(l.Log.Entries)) {
			// This follower is now up-to-date or is being
			// updated by a commit AppendLogs message.
			return true
		}
	} else {
		l.followerKnownLogIndices[followerIndex] = resp.CommitIndex
		l.followerSentLogIndices[followerIndex] = resp.CommitIndex
	}

	// If we are here, there is more to send to this follower,
	// either because it failed to handle the last message, or
	// because new entries have been added since.
	msg = l.appendLogsForFollower(followerIndex)
	rawMsg := &simulator.Message{
		Source:  l.Port,
		Dest:    l.Followers[followerIndex],
		Message: msg,
		Size:    float64(msg.Size()),
	}
	l.Network.Send(l.Handle, rawMsg)

	return true
}

func (l *Leader[C, S]) handleCommand(port *simulator.Port, command *CommandMessage[C]) {
	idx := l.Log.Append(l.Term, command.Command)
	l.callbacks[idx] = port
	l.commandIDs[idx] = command.ID
	l.sendFreshAppendLogs()
}

func (l *Leader[C, S]) sendFreshAppendLogs() {
	for i, lastSent := range l.lastSeqNums {
		if lastSent != 0 {
			// A message is in flight, so we won't send a new one
			// until we get an ack.
			continue
		}
		l.lastSentTime[i] = math.Inf(-1)
	}
	l.sendAppendLogs()
}

func (l *Leader[C, S]) sendAppendLogs() {
	messages := make([]*simulator.Message, 0, len(l.Followers))
	for i, port := range l.Followers {
		if l.Handle.Time() < l.lastSentTime[i]+l.HeartbeatInterval/2 {
			// Don't send redundant messages if a keepalive is unneeded.
			continue
		}
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

// appendLogsForFollower creates an AppendLogs message and
// updates our message book-keeping under the assumption
// that the message will be sent.
func (l *Leader[C, S]) appendLogsForFollower(i int) *RaftMessage[C, S] {
	logIndex := l.followerSentLogIndices[i]
	msg := &RaftMessage[C, S]{AppendLogs: &AppendLogs[C, S]{
		Term:        l.Term,
		SeqNum:      l.seqNum,
		CommitIndex: l.Log.OriginIndex,
	}}

	// Book-keeping under the assumption that we will send
	// this message.
	l.lastSeqNums[i] = l.seqNum
	l.lastSentTime[i] = l.Handle.Time()
	l.followerSentLogIndices[i] = l.Log.OriginIndex + int64(len(l.Log.Entries))
	l.seqNum++

	if logIndex < l.Log.OriginIndex {
		// We must include the entire state machine.
		msg.AppendLogs.OriginIndex = l.Log.OriginIndex
		msg.AppendLogs.OriginTerm = l.Log.OriginTerm
		originState := l.Log.Origin.Clone()
		msg.AppendLogs.Origin = &originState
		msg.AppendLogs.Entries = append([]LogEntry[C]{}, l.Log.Entries...)
	} else if logIndex > l.Log.OriginIndex {
		msg.AppendLogs.OriginIndex = logIndex
		msg.AppendLogs.OriginTerm = l.Log.Entries[logIndex-l.Log.OriginIndex-1].Term
		msg.AppendLogs.Entries = append(
			[]LogEntry[C]{},
			l.Log.Entries[logIndex-l.Log.OriginIndex:]...,
		)
	} else {
		msg.AppendLogs.OriginIndex = l.Log.OriginIndex
		msg.AppendLogs.OriginTerm = l.Log.OriginTerm
		msg.AppendLogs.Entries = append([]LogEntry[C]{}, l.Log.Entries...)
	}
	return msg
}

func (l *Leader[C, S]) maybeAdvanceCommit() {
	sorted := append([]int64{}, l.followerKnownLogIndices...)
	sort.Slice(sorted, func(i int, j int) bool {
		return sorted[i] < sorted[j]
	})
	minCommit := sorted[len(sorted)/2]

	if minCommit > l.Log.OriginIndex {
		commitEntry := l.Log.Entries[minCommit-l.Log.OriginIndex-1]
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

		l.sendFreshAppendLogs()
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
