package gapi

import (
	db "github.com/1morshed1/distributed-ledger-engine/internal/db"
	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These converters translate between the DB models and the protobuf messages.
// Keeping them in one place means the wire format and the storage format can
// evolve independently.

func toPbAccount(a db.Account) *pb.Account {
	return &pb.Account{
		Id:        a.ID,
		Owner:     a.Owner,
		Balance:   a.Balance,
		Currency:  a.Currency,
		CreatedAt: timestamppb.New(a.CreatedAt.Time),
		UpdatedAt: timestamppb.New(a.UpdatedAt.Time),
	}
}

func toPbEntry(e db.Entry) *pb.Entry {
	return &pb.Entry{
		Id:        e.ID,
		AccountId: e.AccountID,
		Amount:    e.Amount,
		CreatedAt: timestamppb.New(e.CreatedAt.Time),
	}
}

func toPbTransfer(t db.Transfer) *pb.Transfer {
	return &pb.Transfer{
		Id:             t.ID,
		FromAccountId:  t.FromAccountID,
		ToAccountId:    t.ToAccountID,
		Amount:         t.Amount,
		IdempotencyKey: t.IdempotencyKey.String(),
		Status:         t.Status,
		CreatedAt:      timestamppb.New(t.CreatedAt.Time),
	}
}
