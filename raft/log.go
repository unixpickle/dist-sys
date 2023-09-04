package raft

// A LogEntry is an immutable step in a log that mutates or
// returns state about a StateMachine.
type Command interface {
	Size() int
}

type LogEntry[C Command] struct {
	Term    int64
	Command C
}

func (l LogEntry[C]) Size() int {
	return 8 + l.Command.Size()
}

// A StateMachine with log type L stores some state which
// can be mutated by log entries, and each state transition
// may return some value.
type StateMachine[C Command, Self any] interface {
	ApplyState(C) any
	Size() int
	Clone() Self
}

// A Log stores a state machine and log entries which are
// applied to it.
type Log[C Command, S StateMachine[C, S]] struct {
	Origin      S
	OriginIndex int64

	// Log entries starting from origin leading up to
	// latest state.
	Entries     []LogEntry[C]
	LatestState S

	CommitIndex int64
}

// Commit caches log entries before a log index and
// advances the commit index.
func (l *Log[C, S]) Commit(commitIndex int64) {
	if commitIndex == l.OriginIndex {
		return
	}
	for i := l.OriginIndex; i < commitIndex; i++ {
		l.Origin.ApplyState(l.Entries[i+l.OriginIndex].Command)
	}
	l.Entries = append([]LogEntry[C]{}, l.Entries[commitIndex-l.OriginIndex:]...)
	l.OriginIndex = commitIndex
	l.CommitIndex = commitIndex
}
