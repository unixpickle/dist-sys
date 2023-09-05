package raft

// A LogEntry is an immutable step in a log that mutates or
// returns state about a StateMachine.
type Command interface {
	Size() int
}

// Result is some result from a command.
type Result interface {
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
	ApplyState(C) Result
	Size() int
	Clone() Self
}

// A Log stores a state machine and log entries which are
// applied to it.
type Log[C Command, S StateMachine[C, S]] struct {
	// Origin is the state machine up to the last committed
	// state.
	Origin S

	// OriginIndex is the index of the next log index.
	OriginIndex int64

	// OriginTerm is the term of the latest log message
	// that was applied to the state machine.
	OriginTerm int64

	// Log entries starting from origin leading up to
	// latest state.
	Entries []LogEntry[C]
}

// LatestTermAndIndex gets the latest position in the log,
// which can be used for leader election decisions.
func (l *Log[C, S]) LatestTermAndIndex() (int64, int64) {
	if len(l.Entries) == 0 {
		return l.OriginTerm, l.OriginIndex
	} else {
		e := l.Entries[len(l.Entries)-1]
		return e.Term, l.OriginIndex + int64(len(l.Entries))
	}
}

// Commit caches log entries before a log index and
// advances the commit index.
func (l *Log[C, S]) Commit(commitIndex int64) []Result {
	if commitIndex == l.OriginIndex {
		return nil
	}
	var results []Result
	for i := l.OriginIndex; i < commitIndex; i++ {
		results = append(results, l.Origin.ApplyState(l.Entries[i+l.OriginIndex].Command))
	}
	l.Entries = append([]LogEntry[C]{}, l.Entries[commitIndex-l.OriginIndex:]...)
	l.OriginTerm = l.Entries[commitIndex-l.OriginIndex].Term
	l.OriginIndex = commitIndex
	return results
}

// Append adds a command to the log and returns the index
// of the resulting log entry.
func (l *Log[C, S]) Append(term int64, command C) int64 {
	l.Entries = append(l.Entries, LogEntry[C]{
		Term:    term,
		Command: command,
	})
	return l.OriginIndex + int64(len(l.Entries)-1)
}
