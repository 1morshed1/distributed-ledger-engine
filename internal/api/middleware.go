package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/1morshed1/distributed-ledger-engine/internal/token"
	"github.com/gin-gonic/gin"
)

const (
	authHeaderKey  = "authorization"
	authTypeBearer = "bearer"
	authPayloadKey = "authorization_payload" // gin context key for the verified payload
)

// authMiddleware verifies the `Authorization: Bearer <token>` header and stores
// the token payload in the request context for downstream handlers.
func authMiddleware(tokenMaker token.Maker) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		header := ctx.GetHeader(authHeaderKey)
		if len(header) == 0 {
			abortUnauthorized(ctx, errors.New("authorization header is not provided"))
			return
		}

		fields := strings.Fields(header)
		if len(fields) < 2 {
			abortUnauthorized(ctx, errors.New("invalid authorization header format"))
			return
		}
		if strings.ToLower(fields[0]) != authTypeBearer {
			abortUnauthorized(ctx, fmt.Errorf("unsupported authorization type %s", fields[0]))
			return
		}

		payload, err := tokenMaker.VerifyToken(fields[1])
		if err != nil {
			abortUnauthorized(ctx, err)
			return
		}

		ctx.Set(authPayloadKey, payload)
		ctx.Next()
	}
}

func abortUnauthorized(ctx *gin.Context, err error) {
	ctx.AbortWithStatusJSON(http.StatusUnauthorized, errorResponse(err))
}
