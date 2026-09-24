package worker

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

// Queue names let us prioritize work. Asynq pulls from higher-weight queues more
// often, so critical jobs aren't starved by bulk ones.
const (
	QueueCritical = "critical"
	QueueDefault  = "default"
)

// TaskProcessor runs the worker side: it pulls tasks from Redis and dispatches
// them to handlers.
type TaskProcessor interface {
	Start() error
	Shutdown()
}

type RedisTaskProcessor struct {
	server *asynq.Server
}

func NewRedisTaskProcessor(redisOpt asynq.RedisClientOpt) TaskProcessor {
	server := asynq.NewServer(redisOpt, asynq.Config{
		Queues: map[string]int{
			QueueCritical: 10, // 10x more likely to be polled than default
			QueueDefault:  1,
		},
		ErrorHandler: asynq.ErrorHandlerFunc(func(_ context.Context, task *asynq.Task, err error) {
			log.Printf("task failed type=%s error=%v", task.Type(), err)
		}),
	})
	return &RedisTaskProcessor{server: server}
}

// Start registers handlers and begins processing. Non-blocking (asynq runs its
// own goroutines).
func (p *RedisTaskProcessor) Start() error {
	mux := asynq.NewServeMux()
	mux.HandleFunc(TaskTransferNotification, p.ProcessTaskTransferNotification)
	return p.server.Start(mux)
}

func (p *RedisTaskProcessor) Shutdown() {
	p.server.Shutdown()
}
