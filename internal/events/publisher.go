package events

import (
	"context"

	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
)

// Publisher emits domain events to the event stream. The interface keeps callers
// (the async worker) decoupled from Kafka, and lets tests use a fake.
type Publisher interface {
	PublishTransactionEvent(ctx context.Context, event *pb.TransactionEvent) error
	Close() error
}
