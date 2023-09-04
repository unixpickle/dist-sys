package raft

type RaftMessage[L LogEntry, S StateMachine[L, S]] struct {
	AppendLogs         *AppendLogs[L, S]
	AppendLogsResponse *AppendLogsResponse[L, S]
}

func (r *RaftMessage[L, S]) Size() int {
	if r.AppendLogs != nil {
		return r.AppendLogs.Size()
	} else if r.AppendLogsResponse != nil {
		return r.AppendLogsResponse.Size()
	}
	panic("unknown message type")
}

func (r *RaftMessage[L, S]) Term() int64 {
	if r.AppendLogs != nil {
		return r.AppendLogs.Term
	} else if r.AppendLogsResponse != nil {
		return r.AppendLogsResponse.Term
	}
	panic("unknown message type")
}

// AppendLogs is a message sent from leaders to followers.
type AppendLogs[L LogEntry, S StateMachine[L, S]] struct {
	Term        int64
	CommitIndex int64

	OriginIndex int64

	// Origin may be specified if we are too far behind.
	Origin *S

	Entries []L
}

// Size tabulates the approximate number of bytes requires
// to encode this message.
func (a *AppendLogs[L, S]) Size() int {
	res := 8 * 3
	if a.Origin != nil {
		res += (*a.Origin).Size()
	}
	for _, e := range a.Entries {
		res += e.Size()
	}
	return res
}

// AppendLogsResponse is sent by the followers to the
// leader in response to an AppendLogs message.
type AppendLogsResponse[L LogEntry, S StateMachine[L, S]] struct {
	Term        int64
	CommitIndex int64
	LatestIndex int64
}

func (a *AppendLogsResponse[L, S]) Size() int {
	return 8 * 3
}
