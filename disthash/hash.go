package disthash

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"math"
)

// A Hasher is an object which can be turned into raw data
// that quasi-uniquely represents the data.
type Hasher interface {
	Hash() []byte
}

// A GobHasher hashes objects by encoding them as gobs.
type GobHasher struct {
	Object interface{}
}

// Hash returns the gob-encoded data.
func (g *GobHasher) Hash() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(g.Object); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// FloatHash hashes a value into a floating point number
// in the range [0, 1).
func FloatHash(data []byte) float64 {
	digest := md5.Sum(data)
	var number int64
	for i, x := range digest[:8] {
		number |= (int64(x) << uint(8*i))
	}
	return math.Min(math.Nextafter(1, -1), float64(number)/math.Pow(2, 64))
}
