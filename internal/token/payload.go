package token

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrExpiredToken = errors.New("token has expired")
	ErrInvalidToken = errors.New("token is invalid")
)

// Payload is the data carried inside a token. Keep it small — it's sent on every
// request. ID lets us block/track a specific token (e.g. a refresh session).
type Payload struct {
	ID        uuid.UUID `json:"id"`
	Owner     string    `json:"owner"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiredAt time.Time `json:"expired_at"`
}

func NewPayload(owner string, duration time.Duration) (*Payload, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &Payload{
		ID:        id,
		Owner:     owner,
		IssuedAt:  now,
		ExpiredAt: now.Add(duration),
	}, nil
}

// Valid reports whether the token is still within its lifetime.
func (p *Payload) Valid() error {
	if time.Now().After(p.ExpiredAt) {
		return ErrExpiredToken
	}
	return nil
}
