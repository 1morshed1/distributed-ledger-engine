package api

import (
	"errors"
	"fmt"
	"net/http"

	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/token"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const idempotencyHeaderKey = "Idempotency-Key"

type createTransferRequest struct {
	FromAccountID int64  `json:"from_account_id" binding:"required,min=1"`
	ToAccountID   int64  `json:"to_account_id" binding:"required,min=1"`
	Amount        int64  `json:"amount" binding:"required,gt=0"`
	Currency      string `json:"currency" binding:"required,currency"`
}

func (server *Server) createTransfer(ctx *gin.Context) {
	var req createTransferRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	// Idempotency key is required and must be a UUID (client-generated per intent).
	rawKey := ctx.GetHeader(idempotencyHeaderKey)
	if rawKey == "" {
		ctx.JSON(http.StatusBadRequest, errorResponse(fmt.Errorf("%s header is required", idempotencyHeaderKey)))
		return
	}
	idempotencyKey, err := uuid.Parse(rawKey)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(fmt.Errorf("%s must be a valid UUID", idempotencyHeaderKey)))
		return
	}

	// The source account must exist, match the currency, and belong to the caller.
	fromAccount, valid := server.validAccount(ctx, req.FromAccountID, req.Currency)
	if !valid {
		return
	}
	authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)
	if fromAccount.Owner != authPayload.Owner {
		ctx.JSON(http.StatusForbidden, errorResponse(errors.New("from account does not belong to you")))
		return
	}

	// The destination account must exist and match the currency (any owner).
	if _, valid := server.validAccount(ctx, req.ToAccountID, req.Currency); !valid {
		return
	}

	result, err := server.store.TransferTx(ctx, db.TransferTxParams{
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		Amount:         req.Amount,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		switch {
		case errors.Is(err, db.ErrInsufficientFunds):
			ctx.JSON(http.StatusUnprocessableEntity, errorResponse(err))
		case db.ErrorCode(err) == db.UniqueViolation:
			// Duplicate idempotency key -> return the original transfer, 409.
			original, gerr := server.store.GetTransferByIdempotencyKey(ctx, idempotencyKey)
			if gerr != nil {
				ctx.JSON(http.StatusInternalServerError, errorResponse(gerr))
				return
			}
			ctx.JSON(http.StatusConflict, gin.H{
				"message":  "transfer with this idempotency key already processed",
				"transfer": original,
			})
		default:
			ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		}
		return
	}

	// Write-through: push both updated balances into the cache so the next
	// balance read is served from Redis. Best-effort — a cache failure must not
	// fail a committed transfer (the DB is the source of truth).
	if server.cache != nil {
		_ = server.cache.SetBalance(ctx, result.FromAccount.ID, result.FromAccount.Balance)
		_ = server.cache.SetBalance(ctx, result.ToAccount.ID, result.ToAccount.Balance)
	}

	ctx.JSON(http.StatusCreated, result)
}

// validAccount fetches an account and checks the currency matches. On any
// failure it writes the HTTP response and returns valid=false.
func (server *Server) validAccount(ctx *gin.Context, accountID int64, currency string) (db.Account, bool) {
	account, err := server.store.GetAccount(ctx, accountID)
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			ctx.JSON(http.StatusNotFound, errorResponse(fmt.Errorf("account %d not found", accountID)))
			return account, false
		}
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return account, false
	}
	if account.Currency != currency {
		ctx.JSON(http.StatusBadRequest, errorResponse(
			fmt.Errorf("account %d currency mismatch: %s vs %s", accountID, account.Currency, currency)))
		return account, false
	}
	return account, true
}

type listTransfersRequest struct {
	AccountID int64 `form:"account_id" binding:"required,min=1"`
	AfterID   int64 `form:"after_id" binding:"min=0"`      // cursor: last id seen, 0 for first page
	PageSize  int32 `form:"page_size" binding:"required,min=1,max=100"`
}

func (server *Server) listTransfers(ctx *gin.Context) {
	var req listTransfersRequest
	if err := ctx.ShouldBindQuery(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, errorResponse(err))
		return
	}

	// Authorization: the account must belong to the caller.
	account, err := server.store.GetAccount(ctx, req.AccountID)
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			ctx.JSON(http.StatusNotFound, errorResponse(err))
			return
		}
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}
	authPayload := ctx.MustGet(authPayloadKey).(*token.Payload)
	if account.Owner != authPayload.Owner {
		ctx.JSON(http.StatusForbidden, errorResponse(errors.New("account does not belong to you")))
		return
	}

	transfers, err := server.store.ListTransfers(ctx, db.ListTransfersParams{
		AccountID: req.AccountID,
		AfterID:   req.AfterID,
		PageSize:  req.PageSize,
	})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, errorResponse(err))
		return
	}

	// Next-page cursor: the id of the last row returned.
	var nextCursor int64
	if len(transfers) > 0 {
		nextCursor = transfers[len(transfers)-1].ID
	}
	ctx.JSON(http.StatusOK, gin.H{"transfers": transfers, "next_after_id": nextCursor})
}
