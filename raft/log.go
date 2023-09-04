package raft

// A LogEntry is an immutable step in a log that mutates or
// returns state about a StateMachine.
type LogEntry interface {
	Size() int
}

// A StateMachine with log type L stores some state which
// can be mutated by log entries, and each state transition
// may return some value.
type StateMachine[L LogEntry, Self any] interface {
	ApplyState(L) any
	Size() int
	Clone() Self
}

// A Log stores a state machine and log entries which are
// applied to it.
type Log[L LogEntry, S StateMachine[L, S]] struct {
	Origin      S
	OriginIndex int64

	// Log entries starting from origin leading up to
	// latest state.
	Entries     []L
	LatestState S

	CommitIndex int64
}

// Commit caches log entries before a log index and
// advances the commit index.
func (l *Log[L, S]) Commit(commitIndex int64) {
	if commitIndex == l.OriginIndex {
		return
	}
	for i := l.OriginIndex; i < commitIndex; i++ {
		l.Origin.ApplyState(l.Entries[i+l.OriginIndex])
	}
	l.Entries = append([]L{}, l.Entries[commitIndex-l.OriginIndex:]...)
	l.OriginIndex = commitIndex
	l.CommitIndex = commitIndex
}
