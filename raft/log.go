package raft

// A StateMachine with log type T stores some state which
// can be mutated by log entries, and each state transition
// may return some value.
type StateMachine[T any] interface {
	ApplyState(T) any
}

// A Log stores a state machine and log entries which are
// applied to it.
type Log[T any, S StateMachine[T]] struct {
	Origin      S
	OriginIndex int64

	// Log entries starting from origin leading up to
	// latest state.
	Entries     []T
	LatestState S
}

// AdvanceOrigin caches log entries before a log index.
// This should only be called once a log entry is
// committed, since then we know it will never be
// overwritten.
func (l *Log[T, S]) AdvanceOrigin(commitIndex int64) {
	if commitIndex == l.OriginIndex {
		return
	}
	for i := l.OriginIndex; i < commitIndex; i++ {
		l.Origin.ApplyState(l.Entries[i+l.OriginIndex])
	}
	l.Entries = append([]T{}, l.Entries[commitIndex-l.OriginIndex:]...)
	l.OriginIndex = commitIndex
}
