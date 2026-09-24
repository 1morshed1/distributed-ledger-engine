package gapi

import (
	"context"
	"errors"
	"time"

	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
	"github.com/1morshed1/distributed-ledger-engine/internal/util"
	"github.com/1morshed1/distributed-ledger-engine/internal/worker"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CreateAccount(ctx context.Context, req *pb.CreateAccountRequest) (*pb.Account, error) {
	if !util.IsSupportedCurrency(req.GetCurrency()) {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported currency: %s", req.GetCurrency())
	}
	account, err := s.store.CreateAccount(ctx, db.CreateAccountParams{
		Owner:    req.GetOwner(),
		Balance:  0,
		Currency: req.GetCurrency(),
	})
	if err != nil {
		if db.ErrorCode(err) == db.UniqueViolation {
			return nil, status.Error(codes.AlreadyExists, "account already exists for this currency")
		}
		return nil, status.Errorf(codes.Internal, "failed to create account: %v", err)
	}
	return toPbAccount(account), nil
}

func (s *Server) GetAccount(ctx context.Context, req *pb.GetAccountRequest) (*pb.Account, error) {
	account, err := s.store.GetAccount(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			return nil, status.Errorf(codes.NotFound, "account %d not found", req.GetId())
		}
		return nil, status.Errorf(codes.Internal, "failed to get account: %v", err)
	}
	return toPbAccount(account), nil
}

func (s *Server) Transfer(ctx context.Context, req *pb.TransferRequest) (*pb.TransferResult, error) {
	if req.GetAmount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}
	key, err := uuid.Parse(req.GetIdempotencyKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key must be a valid UUID")
	}

	// Both accounts must exist and match the requested currency.
	for _, id := range []int64{req.GetFromAccountId(), req.GetToAccountId()} {
		acc, err := s.store.GetAccount(ctx, id)
		if err != nil {
			if errors.Is(err, db.ErrRecordNotFound) {
				return nil, status.Errorf(codes.NotFound, "account %d not found", id)
			}
			return nil, status.Errorf(codes.Internal, "failed to get account: %v", err)
		}
		if acc.Currency != req.GetCurrency() {
			return nil, status.Errorf(codes.InvalidArgument, "account %d currency mismatch", id)
		}
	}

	result, err := s.store.TransferTx(ctx, db.TransferTxParams{
		FromAccountID:  req.GetFromAccountId(),
		ToAccountID:    req.GetToAccountId(),
		Amount:         req.GetAmount(),
		IdempotencyKey: key,
	})
	if err != nil {
		switch {
		case errors.Is(err, db.ErrInsufficientFunds):
			return nil, status.Error(codes.FailedPrecondition, "insufficient funds")
		case db.ErrorCode(err) == db.UniqueViolation:
			return nil, status.Error(codes.AlreadyExists, "transfer already processed for this idempotency key")
		default:
			return nil, status.Errorf(codes.Internal, "transfer failed: %v", err)
		}
	}

	// Write-through cache, best-effort.
	if s.cache != nil {
		_ = s.cache.SetBalance(ctx, result.FromAccount.ID, result.FromAccount.Balance)
		_ = s.cache.SetBalance(ctx, result.ToAccount.ID, result.ToAccount.Balance)
	}

	// Async follow-up, best-effort.
	if s.taskDistributor != nil {
		_ = s.taskDistributor.DistributeTaskTransferNotification(ctx,
			&worker.TransferNotificationPayload{
				TransferID:    result.Transfer.ID,
				FromAccountID: result.Transfer.FromAccountID,
				ToAccountID:   result.Transfer.ToAccountID,
				Amount:        result.Transfer.Amount,
			},
			asynq.MaxRetry(3),
			asynq.Queue(worker.QueueCritical),
		)
		_ = s.taskDistributor.DistributeTaskPublishTransferEvent(ctx,
			&worker.PublishTransferEventPayload{
				TransferID:    result.Transfer.ID,
				FromAccountID: result.Transfer.FromAccountID,
				ToAccountID:   result.Transfer.ToAccountID,
				Amount:        result.Transfer.Amount,
				Currency:      req.GetCurrency(),
				OccurredAt:    time.Now(),
			},
			asynq.MaxRetry(5),
			asynq.Queue(worker.QueueCritical),
		)
	}

	return &pb.TransferResult{
		Transfer:    toPbTransfer(result.Transfer),
		FromAccount: toPbAccount(result.FromAccount),
		ToAccount:   toPbAccount(result.ToAccount),
		FromEntry:   toPbEntry(result.FromEntry),
		ToEntry:     toPbEntry(result.ToEntry),
	}, nil
}

func (s *Server) ListTransfers(ctx context.Context, req *pb.ListTransfersRequest) (*pb.ListTransfersResponse, error) {
	pageSize := req.GetPageSize()
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	transfers, err := s.store.ListTransfers(ctx, db.ListTransfersParams{
		AccountID: req.GetAccountId(),
		AfterID:   req.GetAfterId(),
		PageSize:  pageSize,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list transfers: %v", err)
	}

	resp := &pb.ListTransfersResponse{}
	for _, t := range transfers {
		resp.Transfers = append(resp.Transfers, toPbTransfer(t))
	}
	if n := len(transfers); n > 0 {
		resp.NextAfterId = transfers[n-1].ID
	}
	return resp, nil
}

// GetAccountBalance is the hot path used by the Spark fraud job. Cache first.
func (s *Server) GetAccountBalance(ctx context.Context, req *pb.GetBalanceRequest) (*pb.BalanceResponse, error) {
	id := req.GetAccountId()

	if s.cache != nil {
		if balance, hit, err := s.cache.GetBalance(ctx, id); err == nil && hit {
			// Still need currency; fetch the row but return the cached balance.
			if acc, aerr := s.store.GetAccount(ctx, id); aerr == nil {
				return &pb.BalanceResponse{AccountId: id, Balance: balance, Currency: acc.Currency}, nil
			}
		}
	}

	acc, err := s.store.GetAccount(ctx, id)
	if err != nil {
		if errors.Is(err, db.ErrRecordNotFound) {
			return nil, status.Errorf(codes.NotFound, "account %d not found", id)
		}
		return nil, status.Errorf(codes.Internal, "failed to get balance: %v", err)
	}
	if s.cache != nil {
		_ = s.cache.SetBalance(ctx, acc.ID, acc.Balance)
	}
	return &pb.BalanceResponse{AccountId: acc.ID, Balance: acc.Balance, Currency: acc.Currency}, nil
}
