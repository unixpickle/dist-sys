// Package disthash implements distributed hashing
// algorithms.
package disthash

import (
	"github.com/unixpickle/dist-sys/simulator"
)

// A DistHash is a distributed hashing algorithm.
// It assigns different keys to different nodes.
type DistHash interface {
	// AddSite adds a bucket to the hash table.
	AddSite(node *simulator.Node, id Hasher) Hasher

	// RemoveSite removes a bucket from the hash table.
	RemoveSite(node *simulator.Node)

	// Sites returns all buckets in the hash table.
	Sites() []*simulator.Node

	// KeySites returns the buckets where a key is stored.
	//
	// This must return at least one node, provided there
	// are any nodes in the hash table.
	KeySites(key Hasher) []*simulator.Node
}
