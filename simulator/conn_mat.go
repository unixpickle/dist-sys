package simulator

// A ConnMat is a connectivity matrix.
//
// Entries in the matrix indicate a transfer rate from a
// source node (row) to a destination node (column).
type ConnMat struct {
	numNodes int
	rates    []float64
}

// NewConnMat creates an all-zero connection matrix.
func NewConnMat(numNodes int) *ConnMat {
	return &ConnMat{
		numNodes: numNodes,
		rates:    make([]float64, numNodes*numNodes),
	}
}

// NumNodes returns the number of nodes.
func (c *ConnMat) NumNodes() int {
	return c.numNodes
}

// Get an entry in the matrix.
func (c *ConnMat) Get(src, dst int) float64 {
	if src < 0 || dst < 0 || src >= c.numNodes || dst >= c.numNodes {
		panic("index out of bounds")
	}
	return c.rates[src*c.numNodes+dst]
}

// Set an entry in the matrix.
func (c *ConnMat) Set(src, dst int, value float64) {
	if src < 0 || dst < 0 || src >= c.numNodes || dst >= c.numNodes {
		panic("index out of bounds")
	}
	c.rates[src*c.numNodes+dst] = value
}

// SumDest sums a column of the matrix.
func (c *ConnMat) SumDest(dst int) float64 {
	if dst < 0 || dst >= c.numNodes {
		panic("index out of bounds")
	}
	var sum float64
	for i := 0; i < c.numNodes; i++ {
		sum += c.Get(i, dst)
	}
	return sum
}

// SumSource sums a row of the matrix.
func (c *ConnMat) SumSource(src int) float64 {
	if src < 0 || src >= c.numNodes {
		panic("index out of bounds")
	}
	var sum float64
	for i := 0; i < c.numNodes; i++ {
		sum += c.Get(src, i)
	}
	return sum
}

// ScaleDest scales a column of the matrix.
func (c *ConnMat) ScaleDest(dst int, scale float64) {
	if dst < 0 || dst >= c.numNodes {
		panic("index out of bounds")
	}
	for i := 0; i < c.numNodes; i++ {
		c.Set(i, dst, c.Get(i, dst)*scale)
	}
}

// ScaleSource scales a row of the matrix.
func (c *ConnMat) ScaleSource(src int, scale float64) {
	if src < 0 || src >= c.numNodes {
		panic("index out of bounds")
	}
	for i := 0; i < c.numNodes; i++ {
		c.Set(src, i, c.Get(src, i)*scale)
	}
}
