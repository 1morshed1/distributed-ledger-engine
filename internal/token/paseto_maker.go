package token

import (
	"encoding/json"
	"fmt"
	"time"

	"aidanwoods.dev/go-paseto"
)

// PasetoMaker implements Maker using PASETO v4.local (symmetric encryption).
//
// Why PASETO over JWT: JWT lets the token itself declare its algorithm in the
// header ("alg"), which enables the classic "alg=none" and RS256->HS256 confusion
// attacks. PASETO bakes the algorithm into the version (v4.local), so a token
// can't lie about how it should be verified.
//
// v4.local = symmetric: one 32-byte secret both signs (encrypts) and verifies.
// Fine for a single service issuing its own tokens. If other services needed to
// verify without holding the secret, we'd switch to v4.public (asymmetric) — the
// Maker interface makes that a drop-in change.
type PasetoMaker struct {
	symmetricKey paseto.V4SymmetricKey
	parser       paseto.Parser
}

const chachaKeySize = 32

func NewPasetoMaker(symmetricKey string) (Maker, error) {
	if len(symmetricKey) != chachaKeySize {
		return nil, fmt.Errorf("invalid key size: must be exactly %d bytes", chachaKeySize)
	}
	key, err := paseto.V4SymmetricKeyFromBytes([]byte(symmetricKey))
	if err != nil {
		return nil, err
	}
	// No-rule parser: it only decrypts + authenticates. Expiry is enforced by
	// Payload.Valid() so we can return a precise ErrExpiredToken.
	return &PasetoMaker{symmetricKey: key, parser: paseto.NewParserWithoutExpiryCheck()}, nil
}

func (m *PasetoMaker) CreateToken(owner string, duration time.Duration) (string, *Payload, error) {
	payload, err := NewPayload(owner, duration)
	if err != nil {
		return "", nil, err
	}

	token := paseto.NewToken()
	// Embed our full payload as a claim so VerifyToken returns the same struct.
	// We validate expiry ourselves (Payload.Valid) so we can return a precise
	// ErrExpiredToken rather than a generic parse failure.
	body, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}
	token.SetString("payload", string(body))

	encrypted := token.V4Encrypt(m.symmetricKey, nil)
	return encrypted, payload, nil
}

func (m *PasetoMaker) VerifyToken(encrypted string) (*Payload, error) {
	// ParseV4Local decrypts and authenticates the token. A tampered or
	// wrong-key token fails here.
	parsed, err := m.parser.ParseV4Local(m.symmetricKey, encrypted, nil)
	if err != nil {
		return nil, ErrInvalidToken
	}

	body, err := parsed.GetString("payload")
	if err != nil {
		return nil, ErrInvalidToken
	}
	var payload Payload
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return nil, ErrInvalidToken
	}
	// Expiry check lives here -> returns ErrExpiredToken specifically.
	if err := payload.Valid(); err != nil {
		return nil, err
	}
	return &payload, nil
}
