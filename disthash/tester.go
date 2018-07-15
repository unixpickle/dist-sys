package disthash

import (
	"reflect"
	"testing"

	"github.com/unixpickle/dist-sys/simulator"
	"github.com/unixpickle/essentials"
)

// TestDistHash runs a battery of tests on a DistHash.
func TestDistHash(t *testing.T, maker func() DistHash) {
	t.Run("StaticConsistency", func(t *testing.T) {
		TestStaticConsistency(t, maker())
	})
	t.Run("RemovalConsistency", func(t *testing.T) {
		TestRemovalConsistency(t, maker())
	})
	t.Run("AddConsistency", func(t *testing.T) {
		TestAddConsistency(t, maker())
	})
}

// TestStaticConsistency checks that a DistHash produces
// the same bins when no sites are added/removed.
func TestStaticConsistency(t *testing.T, d DistHash) {
	site1 := simulator.NewNode()
	site2 := simulator.NewNode()
	d.AddSite(site1, &GobHasher{V: "hi"})
	d.AddSite(site2, &GobHasher{V: "hey"})

	sites := make([][][]*simulator.Node, 2)
	for i := 0; i < 2; i++ {
		sites[i] = [][]*simulator.Node{}
		for j := 0; j < 100; j++ {
			keySites := copySites(d.KeySites(&GobHasher{V: j}))
			if len(keySites) == 0 {
				t.Error("no sites for key")
			}
			sites[i] = append(sites[i], keySites)
		}
	}
	if !reflect.DeepEqual(sites[0], sites[1]) {
		t.Error("sites were inconsistent")
	}
}

// TestRemovalConsistency checks that a DistHash does not
// mess with objects stored in sites that are not removed,
// even when other sites are removed.
func TestRemovalConsistency(t *testing.T, d DistHash) {
	var sites []*simulator.Node
	for i := 0; i < 3; i++ {
		site := simulator.NewNode()
		sites = append(sites, site)
		d.AddSite(site, &GobHasher{V: i})
	}

	oldSites := [][]*simulator.Node{}
	for i := 0; i < 100; i++ {
		keySites := copySites(d.KeySites(&GobHasher{V: i}))
		if len(keySites) == 0 {
			t.Error("no sites for key")
		}
		oldSites = append(oldSites, keySites)
	}

	d.RemoveSite(sites[0])

	for i := 0; i < 100; i++ {
		newSites := copySites(d.KeySites(&GobHasher{V: i}))
		if !essentials.Contains(oldSites[i], sites[0]) {
			if !reflect.DeepEqual(oldSites[i], newSites) {
				t.Errorf("site %d should be unaffected, but went from %v to %v",
					i, oldSites[i], newSites)
			}
		} else {
			if len(newSites) == 0 {
				t.Error("no sites for key")
			} else if essentials.Contains(newSites, sites[0]) {
				t.Error("removed site still in use")
			}
		}
	}
}

// TestAddConsistency checks that a DistHash does not move
// keys around excessively when adding new sites.
func TestAddConsistency(t *testing.T, d DistHash) {
	var sites []*simulator.Node
	for i := 0; i < 3; i++ {
		site := simulator.NewNode()
		sites = append(sites, site)
		d.AddSite(site, &GobHasher{V: i})
	}

	oldSites := [][]*simulator.Node{}
	for i := 0; i < 100; i++ {
		keySites := copySites(d.KeySites(&GobHasher{V: i}))
		if len(keySites) == 0 {
			t.Error("no sites for key")
		}
		oldSites = append(oldSites, keySites)
	}

	newSite := simulator.NewNode()
	d.AddSite(newSite, &GobHasher{V: -1})

	for i := 0; i < 100; i++ {
		newSites := copySites(d.KeySites(&GobHasher{V: i}))
		if !essentials.Contains(newSites, newSite) {
			if !reflect.DeepEqual(oldSites[i], newSites) {
				t.Errorf("site %d should be unaffected, but went from %v to %v",
					i, oldSites[i], newSites)
			}
		} else if len(newSites) == 0 {
			t.Error("no sites for key")
		}
	}
}

// copySites copies a slice in such a way that
// reflect.DeepEqual will not be confused by unused
// capacity.
func copySites(sites []*simulator.Node) []*simulator.Node {
	return append([]*simulator.Node{}, sites...)
}
