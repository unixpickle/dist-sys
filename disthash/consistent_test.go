package disthash

import (
	"fmt"
	"testing"
)

func TestConsistent(t *testing.T) {
	for _, size := range []int{1, 10, 200} {
		t.Run(fmt.Sprintf("Size%d", size), func(t *testing.T) {
			TestDistHash(t, func() DistHash {
				return NewConsistent(size)
			})
		})
	}
}
