package proxy

import "sync/atomic"

type portSelector struct {
	min  int
	max  int
	next atomic.Uint64
}

func newPortSelector(min, max int) *portSelector {
	if min <= 0 || max <= 0 || min > max {
		panic("invalid upstream port range")
	}
	return &portSelector{min: min, max: max}
}

func (s *portSelector) pick(blocked func(int) bool) (int, bool) {
	span := s.max - s.min + 1
	start := int((s.next.Add(1) - 1) % uint64(span))
	fallback := 0
	haveFallback := false

	for i := 0; i < span; i++ {
		port := s.min + ((start + i) % span)
		if blocked == nil || !blocked(port) {
			return port, true
		}
		if !haveFallback {
			fallback = port
			haveFallback = true
		}
	}

	if haveFallback {
		return fallback, true
	}
	return 0, false
}
