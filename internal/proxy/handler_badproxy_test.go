package proxy

import (
	"testing"
	"time"
)

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

func TestSelectPort_PrefersHealthyOverPenalized(t *testing.T) {
	getPool := NewUpstreamGETPool("example.test", 500*time.Millisecond, []Credential{{User: "u", Pass: "p"}}, 10001, 10002, time.Second, time.Minute)
	connectPool := NewUpstreamCONNECTPool("example.test", 500*time.Millisecond, []Credential{{User: "u", Pass: "p"}}, 10001, 10002, 45*time.Second, time.Second, time.Minute)
	h := NewHandler(HandlerConfig{
		MaxRetries403: 0,
		Timeout:       1 * time.Second,
		ServiceUser:   "svc",
		ServicePass:   "svc-pass",
		GETPool:       getPool,
		CONNECTPool:   connectPool,
		Creds:         NewCredentialProvider([]Credential{{User: "u", Pass: "p"}}),
	})

	h.badPorts.MarkFailure(10001)
	port := h.selectPort(0, false)
	if port != 10002 {
		t.Fatalf("expected healthy port 10002, got %d", port)
	}
}
