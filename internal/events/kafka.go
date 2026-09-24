package events

import (
	"context"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/1morshed1/distributed-ledger-engine/internal/pb"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// KafkaPublisher writes TransactionEvents to Kafka.
//
// Key producer decisions (see docs/phase-2):
//   - RequiredAcks = RequireAll: the write only succeeds once all in-sync
//     replicas have it. For money, we never trade durability for latency.
//   - Balancer = Hash on the message Key (= from_account_id): all events for one
//     account land on the same partition, so they stay strictly ordered — which
//     fraud detection relies on.
//   - Retries with backoff so a brief broker hiccup doesn't drop an event.
type KafkaPublisher struct {
	writer  *kafka.Writer
	brokers []string
	topic   string
}

func NewKafkaPublisher(brokers string, topic string) *KafkaPublisher {
	brokerList := strings.Split(brokers, ",")
	return &KafkaPublisher{
		brokers: brokerList,
		topic:   topic,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokerList...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // partition by Key
			RequiredAcks:           kafka.RequireAll,
			MaxAttempts:            3,
			AllowAutoTopicCreation: true, // dev convenience
			WriteTimeout:           10 * time.Second,
		},
	}
}

// EnsureTopic creates the topic (idempotent) so the first publish doesn't race
// auto-creation. 3 partitions demonstrates the account-keyed partitioning.
func (p *KafkaPublisher) EnsureTopic(partitions int) error {
	conn, err := kafka.Dial("tcp", p.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	// CreateTopics is a no-op if the topic already exists.
	return ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             p.topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}

func (p *KafkaPublisher) PublishTransactionEvent(ctx context.Context, event *pb.TransactionEvent) error {
	value, err := proto.Marshal(event)
	if err != nil {
		return err
	}

	// Key = from_account_id -> per-account ordering within a partition.
	key := strconv.FormatInt(event.GetPayload().GetFromAccountId(), 10)

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
