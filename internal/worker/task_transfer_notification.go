package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
)

// TaskTransferNotification is the task type name Asynq routes on.
const TaskTransferNotification = "task:transfer_notification"

// TransferNotificationPayload is what the enqueuer serializes into the task.
type TransferNotificationPayload struct {
	TransferID    int64 `json:"transfer_id"`
	FromAccountID int64 `json:"from_account_id"`
	ToAccountID   int64 `json:"to_account_id"`
	Amount        int64 `json:"amount"`
}

func (d *RedisTaskDistributor) DistributeTaskTransferNotification(
	ctx context.Context,
	payload *TransferNotificationPayload,
	opts ...asynq.Option,
) error {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	task := asynq.NewTask(TaskTransferNotification, jsonPayload, opts...)
	info, err := d.client.EnqueueContext(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}
	log.Printf("enqueued task=%s queue=%s id=%s", task.Type(), info.Queue, info.ID)
	return nil
}

// ProcessTaskTransferNotification is the handler run by the worker. In a real
// system this would send an email / push notification / generate a statement.
// If it returns an error, Asynq retries with backoff (see the MaxRetry option).
func (p *RedisTaskProcessor) ProcessTaskTransferNotification(ctx context.Context, task *asynq.Task) error {
	var payload TransferNotificationPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		// A non-retryable error: bad payload will never succeed on retry.
		return fmt.Errorf("failed to unmarshal payload: %w: %w", err, asynq.SkipRetry)
	}

	log.Printf("processed task=%s transfer_id=%d amount=%d (notification sent)",
		task.Type(), payload.TransferID, payload.Amount)
	return nil
}
