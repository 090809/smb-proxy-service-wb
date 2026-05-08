package proxy

import (
	"sync"
	"testing"
	"time"
)

type sequencePicker struct {
	mu   sync.Mutex
	seq  []int
	next int
}

func (p *sequencePicker) Pick(used map[int]bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < len(p.seq); i++ {
		idx := (p.next + i) % len(p.seq)
		port := p.seq[idx]
		if !used[port] {
			p.next = idx + 1
			return port
		}
	}
	return p.seq[0]
}

func TestBadPortState_ExponentialCooldownWithCap(t *testing.T) {
	state := newBadPortState(10*time.Millisecond, 40*time.Millisecond)

	d1 := state.MarkFailure(10001)
	d2 := state.MarkFailure(10001)
	d3 := state.MarkFailure(10001)
	d4 := state.MarkFailure(10001)

	if d1 != 10*time.Millisecond {
		t.Fatalf("unexpected d1: %s", d1)
	}
	if d2 != 20*time.Millisecond {
		t.Fatalf("unexpected d2: %s", d2)
	}
	if d3 != 40*time.Millisecond {
		t.Fatalf("unexpected d3: %s", d3)
	}
	if d4 != 40*time.Millisecond {
		t.Fatalf("unexpected d4: %s", d4)
	}
}

func TestPickPort_PrefersHealthyOverPenalized(t *testing.T) {
	picker := &sequencePicker{seq: []int{10001, 10002}}
	h := NewHandler(HandlerConfig{
		UpstreamHost:        "example.test",
		Picker:              picker,
		MaxRetries403:       0,
		Timeout:             1 * time.Second,
		DialTimeout:         500 * time.Millisecond,
		BadProxyPickSamples: 2,
		ServiceUser:         "svc",
		ServicePass:         "svc-pass",
		Creds:               NewCredentialProvider([]Credential{{User: "u", Pass: "p"}}),
	})

	h.badPorts.MarkFailure(10001)
	port := h.pickPort(map[int]bool{}, 0, false)
	if port != 10002 {
		t.Fatalf("expected healthy port 10002, got %d", port)
	}
}
