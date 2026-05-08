package proxy

import (
	"sync/atomic"
)

type Credential struct {
	User string
	Pass string
}

type CredentialProvider struct {
	creds []Credential
	rr    atomic.Uint64
}

func NewCredentialProvider(creds []Credential) *CredentialProvider {
	if len(creds) == 0 {
		panic("creds must not be empty")
	}
	return &CredentialProvider{creds: creds}
}

// Next returns (index, cred) using round-robin selection.
func (p *CredentialProvider) Next() (int, Credential) {
	n := p.rr.Add(1) - 1
	idx := int(n % uint64(len(p.creds)))
	return idx, p.creds[idx]
}
