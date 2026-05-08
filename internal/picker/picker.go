package picker

import (
	"fmt"
	"math/rand/v2"
)

type Picker struct {
	min int
	max int
}

func New(min, max int) *Picker {
	if min > max {
		panic(fmt.Sprintf("invalid range: %d-%d", min, max))
	}
	return &Picker{min: min, max: max}
}

// Pick returns a port in [min, max] that is not in used.
// If everything is used (unlikely with small attempts), returns a random port.
func (p *Picker) Pick(used map[int]bool) int {
	span := p.max - p.min + 1

	// Random sampling first
	for range 20 {
		port := p.min + rand.IntN(span)
		if !used[port] {
			return port
		}
	}

	// Fallback to linear scan
	for port := p.min; port <= p.max; port++ {
		if !used[port] {
			return port
		}
	}

	return p.min + rand.IntN(span)
}
