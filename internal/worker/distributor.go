package worker

import (
	"context"

	"github.com/hibiken/asynq"
)

// TaskDistributor enqueues background jobs. Handlers implement it so callers
// (REST/gRPC) depend on the interface, not on Redis/Asynq directly — and tests
// can substitute a fake.
type TaskDistributor interface {
	DistributeTaskTransferNotification(
		ctx context.Context,
		payload *TransferNotificationPayload,
		opts ...asynq.Option,
	) error
	DistributeTaskPublishTransferEvent(
		ctx context.Context,
		payload *PublishTransferEventPayload,
		opts ...asynq.Option,
	) error
}

// RedisTaskDistributor is the Asynq-backed implementation. Asynq stores the queue
// in Redis, so we reuse the same Redis instance as the balance cache.
type RedisTaskDistributor struct {
	client *asynq.Client
}

func NewRedisTaskDistributor(redisOpt asynq.RedisClientOpt) TaskDistributor {
	return &RedisTaskDistributor{client: asynq.NewClient(redisOpt)}
}
