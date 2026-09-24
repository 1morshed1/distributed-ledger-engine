package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const TaskPublishTransferEvent = "task:publish_transfer_event"

// PublishTransferEventPayload is the async job that publishes a completed
// transfer to Kafka. Doing it via Asynq (not inline) means a Kafka outage retries
// with backoff instead of failing/blocking the transfer response.
type PublishTransferEventPayload struct {
	TransferID    int64     `json:"transfer_id"`
	FromAccountID int64     `json:"from_account_id"`
	ToAccountID   int64     `json:"to_account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	TraceID       string    `json:"trace_id"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func (d *RedisTaskDistributor) DistributeTaskPublishTransferEvent(
	ctx context.Context,
	payload *PublishTransferEventPayload,
	opts ...asynq.Option,
) error {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	task := asynq.NewTask(TaskPublishTransferEvent, jsonPayload, opts...)
	info, err := d.client.EnqueueContext(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}
	log.Printf("enqueued task=%s queue=%s id=%s", task.Type(), info.Queue, info.ID)
	return nil
}

// ProcessTaskPublishTransferEvent builds the protobuf event and publishes it to
// Kafka. A publish error is returned so Asynq retries (guaranteed delivery).
func (p *RedisTaskProcessor) ProcessTaskPublishTransferEvent(ctx context.Context, task *asynq.Task) error {
	var payload PublishTransferEventPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("bad payload: %w: %w", err, asynq.SkipRetry)
	}

	if p.publisher == nil {
		return fmt.Errorf("no event publisher configured: %w", asynq.SkipRetry)
	}

	event := &pb.TransactionEvent{
		EventId:   uuid.NewString(),
		EventType: "TRANSFER_COMPLETED",
		Timestamp: timestamppb.New(payload.OccurredAt),
		Payload: &pb.TransferPayload{
			TransferId:    payload.TransferID,
			FromAccountId: payload.FromAccountID,
			ToAccountId:   payload.ToAccountID,
			Amount:        payload.Amount,
			Currency:      payload.Currency,
		},
		Metadata: &pb.EventMetadata{
			SourceService: "ledger-api",
			TraceId:       payload.TraceID,
			SchemaVersion: 1,
		},
	}

	if err := p.publisher.PublishTransactionEvent(ctx, event); err != nil {
		// Returned -> Asynq retries with backoff.
		return fmt.Errorf("failed to publish event: %w", err)
	}

	log.Printf("published transaction event transfer_id=%d to kafka", payload.TransferID)
	return nil
}
