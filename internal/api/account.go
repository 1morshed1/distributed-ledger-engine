package api

import (
	"errors"
	"net/http"

	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/token"
	"github.com/gin-gonic/gin"
)

type createAccountRequest struct {
	Currency string `json:"currency" binding:"required,currency"`
}

func (server *Server) createAccount(ctx *gin.Context) {
	var req createAccountRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	// Owner comes from the token, never the request body — a user can only
	// create accounts for themselves.
	authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)

	account, err := server.store.CreateAccount(ctx, db.CreateAccountParams{
		Owner:    authPayload.Owner,
		Balance:  0,
		Currency: req.Currency,
	})
	if err != nil {
		// Unique (owner, currency) -> user already has an account in this currency.
		if db.ErrorCode(err) == db.UniqueViolation {
			ctx.JSON(http.StatusConflict, errorResponse(errors.New("account already exists for this currency")))
			return
		}
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	ctx.JSON(http.StatusCreated, account)
}

type getAccountRequest struct {
	ID int64 `uri:"id" binding:"required,min=1"`
}

func (server *Server) getAccount(ctx *gin.Context) {
	var req getAccountRequest
	if err := ctx.ShouldBindUri(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	account, err := server.store.GetAccount(ctx, req.ID)
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			ctx.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	// Authorization: you can only see your own account.
	authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)
	if account.Owner != authPayload.Owner {
		ctx.JSON(http.StatusForbidden, errorResponse(errors.New("account does not belong to you")))
		return
	}

	ctx.JSON(http.StatusOK, account)
}

// getAccountBalance serves the hot-path balance read: Redis first, Postgres on
// miss (then repopulate the cache). This is the write-through cache's read side.
func (server *Server) getAccountBalance(ctx *gin.Context) {
	var req getAccountRequest
	if err := ctx.ShouldBindUri(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	// We still need the DB row once to authorize ownership (cache holds only the
	// balance, not the owner). After that, repeated reads hit the cache.
	if server.cache != nil {
		if balance, hit, err := server.cache.GetBalance(ctx, req.ID); err == nil && hit {
			// Authorize using the row, but the balance came from cache.
			account, aerr := server.store.GetAccount(ctx, req.ID)
			if aerr == nil && server.authorizeAccount(ctx, account) {
				ctx.JSON(http.StatusOK, gin.H{"account_id": req.ID, "balance": balance, "source": "cache"})
			}
			return
		}
	}

	account, err := server.store.GetAccount(ctx, req.ID)
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			ctx.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}
	if !server.authorizeAccount(ctx, account) {
		return
	}

	// Populate cache on miss (best-effort; a cache error must not fail the read).
	if server.cache != nil {
		_ = server.cache.SetBalance(ctx, account.ID, account.Balance)
	}
	ctx.JSON(http.StatusOK, gin.H{"account_id": account.ID, "balance": account.Balance, "source": "db"})
}

// authorizeAccount enforces that the caller owns the account, writing a 403 and
// returning false otherwise.
func (server *Server) authorizeAccount(ctx *gin.Context, account db.Account) bool {
	authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)
	if account.Owner != authPayload.Owner {
		ctx.JSON(http.StatusForbidden, errorResponse(errors.New("account does not belong to you")))
		return false
	}
	return true
}

type listAccountsRequest struct {
	PageID   int32 `form:"page_id" binding:"required,min=1"`
	PageSize int32 `form:"page_size" binding:"required,min=1,max=100"`
}

func (server *Server) listAccounts(ctx *gin.Context) {
	var req listAccountsRequest
	if err := ctx.ShouldBindQuery(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)
	accounts, err := server.store.ListAccounts(ctx, db.ListAccountsParams{
		Owner:  authPayload.Owner, // only the caller's accounts
		Limit:  req.PageSize,
		Offset: (req.PageID - 1) * req.PageSize,
	})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	ctx.JSON(http.StatusOK, accounts)
}
