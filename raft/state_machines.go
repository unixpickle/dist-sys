package raft

type HashMapCommand struct {
	Key   string
	Value string
}

func (h HashMapCommand) Size() int {
	return 2 + len(h.Key) + len(h.Value)
}

type StringResult struct {
	Value string
}

func (s StringResult) Size() int {
	return len(s.Value)
}

type HashMap struct {
	mapping map[string]string
}

func (h *HashMap) ApplyState(command HashMapCommand) Result {
	if h.mapping == nil {
		h.mapping = map[string]string{}
	}
	if command.Value == "" {
		x, _ := h.mapping[command.Key]
		return StringResult{x}
	}
	h.mapping[command.Key] = command.Value
	return StringResult{command.Value}
}

func (h *HashMap) Size() int {
	if h.mapping == nil {
		h.mapping = map[string]string{}
	}
	// One stop character for the whole map and each key / value
	size := 1
	for k, v := range h.mapping {
		size += len(k) + len(v) + 2
	}
	return size
}

func (h *HashMap) Clone() *HashMap {
	if h.mapping == nil {
		h.mapping = map[string]string{}
	}
	m := map[string]string{}
	for k, v := range h.mapping {
		m[k] = v
	}
	return &HashMap{mapping: m}
}
