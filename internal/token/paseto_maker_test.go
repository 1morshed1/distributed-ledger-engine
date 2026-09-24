package token

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPasetoMaker(t *testing.T) {
	maker, err := NewPasetoMaker("12345678901234567890123456789012") // 32 bytes
	require.NoError(t, err)

	owner := uuid.NewString()
	token, payload, err := maker.CreateToken(owner, time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.Equal(t, owner, payload.Owner)

	verified, err := maker.VerifyToken(token)
	require.NoError(t, err)
	require.Equal(t, owner, verified.Owner)
	require.Equal(t, payload.ID, verified.ID)
}

func TestExpiredToken(t *testing.T) {
	maker, err := NewPasetoMaker("12345678901234567890123456789012")
	require.NoError(t, err)

	token, _, err := maker.CreateToken("bob", -time.Minute) // already expired
	require.NoError(t, err)

	_, err = maker.VerifyToken(token)
	require.ErrorIs(t, err, ErrExpiredToken)
}

func TestInvalidKeySize(t *testing.T) {
	_, err := NewPasetoMaker("tooshort")
	require.Error(t, err)
}

func TestTamperedToken(t *testing.T) {
	maker, _ := NewPasetoMaker("12345678901234567890123456789012")
	token, _, _ := maker.CreateToken("bob", time.Minute)

	// Verify with a DIFFERENT key -> must be rejected.
	other, _ := NewPasetoMaker("abcdefghijabcdefghijabcdefghij12")
	_, err := other.VerifyToken(token)
	require.ErrorIs(t, err, ErrInvalidToken)
}
