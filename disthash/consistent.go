package disthash

import (
	"bytes"
	"encoding/binary"

	"github.com/unixpickle/essentials"

	"github.com/unixpickle/dist-sys/simulator"
)

// Consistent implements consistent hashing.
type Consistent struct {
	nodes     []*circleNode
	numPoints int
}

// NewConsistent creates a consistent hash table that
// generates the given number of points per node.
func NewConsistent(numPoints int) *Consistent {
	return &Consistent{numPoints: numPoints}
}

// AddSite adds a bucket to the hash table.
func (c *Consistent) AddSite(node *simulator.Node, id Hasher) {
	data := id.Hash()
	for i := 0; i < c.numPoints; i++ {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, uint32(i))
		buf.Write(data)
		pos := FloatHash(buf.Bytes())
		c.nodes = append(c.nodes, &circleNode{Node: node, ID: id, Position: pos})
	}
	essentials.VoodooSort(c.nodes, func(i, j int) bool {
		return c.nodes[i].Position < c.nodes[j].Position
	})
}

// RemoveSite removes a bucket from the hash table.
func (c *Consistent) RemoveSite(node *simulator.Node) {
	var newNodes []*circleNode
	for _, point := range c.nodes {
		if point.Node != node {
			newNodes = append(newNodes, point)
		}
	}
	c.nodes = newNodes
}

// Sites returns all buckets in the hash table.
func (c *Consistent) Sites() []*simulator.Node {
	nodes := map[*simulator.Node]bool{}
	var res []*simulator.Node
	for _, point := range c.nodes {
		if !nodes[point.Node] {
			res = append(res, point.Node)
		}
		nodes[point.Node] = true
	}
	return res
}

// KeySites returns the buckets where a key is stored.
//
// This must return at least one node, provided there
// are any nodes in the hash table.
func (c *Consistent) KeySites(key Hasher) []*simulator.Node {
	if len(c.nodes) == 0 {
		return nil
	}
	pos := FloatHash(key.Hash())
	for _, point := range c.nodes {
		if point.Position > pos {
			return []*simulator.Node{point.Node}
		}
	}
	return []*simulator.Node{c.nodes[0].Node}
}

// A circleNode is one node around a circle with a
// circumference of one.
type circleNode struct {
	Node *simulator.Node
	ID   Hasher

	// In the range [0, 1.0).
	Position float64
}
