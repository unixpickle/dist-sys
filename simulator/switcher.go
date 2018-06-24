package simulator

// A Switcher is a switching algorithm that determines how
// rapidly data flows in a graph of nodes.
// One job of the Switcher is to decide how to deal with
// oversubscription.
type Switcher interface {
	// Apply the switching algorithm to compute the
	// transfer rates of every connection.
	//
	// The mat argument is passed in with 1's wherever a
	// node wants to send data to another node, and 0's
	// everywhere else.
	//
	// When the function returns, mat indicates the rate
	// of data between every pair of nodes.
	SwitchedRates(mat *ConnMat)
}

// A GreedyDropSwitcher emulates a switch where outgoing
// data is spread evenly across a node's outputs, and
// inputs to a node are dropped uniformly at random when a
// node is oversubscribed.
//
// This is equivalent to first normalizing the rows of a
// connection matrix, and then normalizing the columns.
type GreedyDropSwitcher struct {
	SendRates []float64
	RecvRates []float64
}

// NewGreedyDropSwitcher creates a GreedyDropSwitcher with
// uniform upload and download rates across all nodes.
func NewGreedyDropSwitcher(numNodes int, rate float64) *GreedyDropSwitcher {
	rates := make([]float64, numNodes)
	for i := range rates {
		rates[i] = rate
	}
	return &GreedyDropSwitcher{
		SendRates: rates,
		RecvRates: rates,
	}
}

// NumNodes gets the number of nodes the switch expects.
func (g *GreedyDropSwitcher) NumNodes() int {
	return len(g.SendRates)
}

// SwitchedRates performs the switching algorithm.
func (g *GreedyDropSwitcher) SwitchedRates(mat *ConnMat) {
	if mat.NumNodes() != g.NumNodes() {
		panic("unexpected number of nodes")
	}

	// Split upload traffic evenly across sockets.
	for src := 0; src < g.NumNodes(); src++ {
		numDests := mat.SumSource(src)
		if numDests > 0 {
			mat.ScaleSource(src, g.SendRates[src]/numDests)
		}
	}

	// Drop download traffic in proportion to the number
	// of incoming packets from each socket.
	for dst := 0; dst < g.NumNodes(); dst++ {
		incomingRate := mat.SumDest(dst)
		if incomingRate > g.RecvRates[dst] {
			mat.ScaleDest(dst, g.RecvRates[dst]/incomingRate)
		}
	}
}
