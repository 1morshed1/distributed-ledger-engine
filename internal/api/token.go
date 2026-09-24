package api

import (
	"errors"
	"net/http"
	"time"

	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/gin-gonic/gin"
)

type renewAccessTokenRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

type renewAccessTokenResponse struct {
	AccessToken          string    `json:"access_token"`
	AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
}

// renewAccessToken issues a fresh access token from a valid refresh token.
// It re-checks the session in the DB so a revoked/blocked session can't mint new
// access tokens even if the refresh token itself is still cryptographically valid.
func (server *Server) renewAccessToken(ctx *gin.Context) {
	var req renewAccessTokenRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	refreshPayload, err := server.tokenMaker.VerifyToken(req.RefreshToken)
	if err != nil {
		ctx.JSON(http.StatusUnauthorized, errorResponse(err))
		return
	}

	session, err := server.store.GetSession(ctx, refreshPayload.ID)
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			ctx.JSON(http.StatusUnauthorized, errorResponse(errors.New("session not found")))
			return
		}
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	switch {
	case session.IsBlocked:
		ctx.JSON(http.StatusUnauthorized, errorResponse(errors.New("session is blocked")))
		return
	case session.Username != refreshPayload.Owner:
		ctx.JSON(http.StatusUnauthorized, errorResponse(errors.New("session user mismatch")))
		return
	case session.RefreshToken != req.RefreshToken:
		// Token doesn't match the stored one -> possible reuse/theft.
		ctx.JSON(http.StatusUnauthorized, errorResponse(errors.New("mismatched refresh token")))
		return
	}

	accessToken, accessPayload, err := server.tokenMaker.CreateToken(
		refreshPayload.Owner, server.config.AccessTokenDuration)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	ctx.JSON(http.StatusOK, renewAccessTokenResponse{
		AccessToken:          accessToken,
		AccessTokenExpiresAt: accessPayload.ExpiredAt,
	})
}
