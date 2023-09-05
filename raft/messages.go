package raft

import "github.com/unixpickle/dist-sys/simulator"

type RaftMessage[C Command, S StateMachine[C, S]] struct {
	AppendLogs         *AppendLogs[C, S]
	AppendLogsResponse *AppendLogsResponse[C, S]
	Vote               *Vote
	VoteResponse       *VoteResponse
}

func (r *RaftMessage[C, S]) Size() int {
	headerSize := 1
	if r.AppendLogs != nil {
		return headerSize + r.AppendLogs.Size()
	} else if r.AppendLogsResponse != nil {
		return headerSize + r.AppendLogsResponse.Size()
	} else if r.Vote != nil {
		return headerSize + r.Vote.Size()
	} else if r.VoteResponse != nil {
		return headerSize + r.VoteResponse.Size()
	}
	panic("unknown message type")
}

func (r *RaftMessage[C, S]) Term() int64 {
	if r.AppendLogs != nil {
		return r.AppendLogs.Term
	} else if r.AppendLogsResponse != nil {
		return r.AppendLogsResponse.Term
	} else if r.Vote != nil {
		return r.Vote.Term
	} else if r.VoteResponse != nil {
		return r.VoteResponse.Term
	}
	panic("unknown message type")
}

// AppendLogs is a message sent from leaders to followers.
type AppendLogs[C Command, S StateMachine[C, S]] struct {
	Term        int64
	CommitIndex int64

	// SeqNum is unique per term and helps leaders
	// determine if this is a stale response.
	SeqNum int64

	// OriginTerm is the term of the message corresponding
	// to the origin.
	OriginTerm int64

	// OriginIndex may be different than CommitIndex if we
	// are sending newer entries to this worker than are in
	// the committed state machine.
	//
	// In this case, it will be greater than CommitIndex,
	// and Origin will be nil.
	OriginIndex int64

	// Origin may be specified if we are too far behind.
	Origin *S

	Entries []LogEntry[C]
}

// Size tabulates the approximate number of bytes requires
// to encode this message.
func (a *AppendLogs[C, S]) Size() int {
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
type AppendLogsResponse[C Command, S StateMachine[C, S]] struct {
	Term        int64
	CommitIndex int64
	LatestIndex int64
	SeqNum      int64

	// Success will be false if there was not enough
	// data to fill in the logs.
	Success bool
}

func (a *AppendLogsResponse[C, S]) Size() int {
	return 8 * 3
}

type Vote struct {
	Term int64

	// Last log message
	LatestTerm  int64
	LatestIndex int64
}

func (v *Vote) Size() int {
	return 8 * 3
}

type VoteResponse struct {
	Term         int64
	ReceivedVote bool
}

func (v *VoteResponse) Size() int {
	return 8 + 1
}

type CommandMessage[C Command] struct {
	Command C
	ID      string
}

type CommandResponse struct {
	ID            string
	Result        Result          // non-nil if Redirect is nil and Unknown is false
	LeaderUnknown bool            // if true, the leader is not known
	Redirect      *simulator.Node // non-nil if this is no longer the leader
}

func (c *CommandResponse) Size() int {
	if c.Result != nil {
		// Result size + header
		return 1 + c.Result.Size()
	} else {
		// IPv4 + port number + header
		return 1 + 6
	}
}
