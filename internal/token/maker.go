package token

import "time"

// Maker abstracts token creation/verification so the auth scheme (PASETO now,
// something else later) can be swapped without touching handlers.
type Maker interface {
	// CreateToken issues a signed token for owner, valid for duration.
	// Returns the token string and the payload embedded in it.
	CreateToken(owner string, duration time.Duration) (string, *Payload, error)

	// VerifyToken checks a token and returns its payload if valid.
	VerifyToken(token string) (*Payload, error)
}
