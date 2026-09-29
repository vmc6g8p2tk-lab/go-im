package messaging

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/example/go-im/internal/metrics"
	"github.com/example/go-im/internal/repository"
	kafka "github.com/segmentio/kafka-go"
)

const Topic = "im.message.created.v1"

// Each instance has its own consumer group so every process can deliver to
// its own local sockets. This is simple but costs O(instances) Kafka reads.
type Kafka struct {
	writer  *kafka.Writer
	reader  *kafka.Reader
	Metrics *metrics.Metrics
}

func New(brokers, instanceID string) *Kafka {
	addresses := strings.Split(brokers, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}
	return &Kafka{
		writer: &kafka.Writer{Addr: kafka.TCP(addresses...), Topic: Topic, Balancer: &kafka.Hash{},
			RequiredAcks: kafka.RequireAll, BatchTimeout: 20 * time.Millisecond, WriteTimeout: 5 * time.Second},
		reader: kafka.NewReader(kafka.ReaderConfig{Brokers: addresses, Topic: Topic,
			GroupID: "im-delivery-" + instanceID, StartOffset: kafka.LastOffset,
			MinBytes: 1, MaxBytes: 1 << 20}),
	}
}

func (k *Kafka) Close() {
	if err := k.reader.Close(); err != nil {
		slog.Warn("close Kafka reader", "error", err)
	}
	if err := k.writer.Close(); err != nil {
		slog.Warn("close Kafka writer", "error", err)
	}
}

func (k *Kafka) Publish(ctx context.Context, event repository.OutboxEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// Recipient ID preserves per-recipient partition order. PostgreSQL IDs and
	// the ACK protocol remain authoritative across retries and replay.
	key := "u:" + strconv.FormatInt(event.RecipientID, 10)
	if event.GroupID > 0 {
		key = "g:" + strconv.FormatInt(event.GroupID, 10)
	} else if event.SenderID > 0 {
		key = "u:" + strconv.FormatInt(event.SenderID, 10)
	}
	err = k.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value})
	if k.Metrics != nil {
		result := "ok"
		value := float64(1)
		if err != nil {
			result = "error"
			value = 0
		}
		k.Metrics.KafkaProducerUp.Set(value)
		k.Metrics.KafkaPublished.WithLabelValues(result).Inc()
	}
	return err
}

func (k *Kafka) RunConsumer(ctx context.Context, handle func(context.Context, repository.OutboxEvent) error) {
	for ctx.Err() == nil {
		message, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if k.Metrics != nil {
				k.Metrics.KafkaConsumerUp.Set(0)
			}
			if ctx.Err() != nil {
				return
			}
			slog.Warn("Kafka fetch failed", "error", err)
			wait(ctx, time.Second)
			continue
		}
		if k.Metrics != nil {
			k.Metrics.KafkaConsumerUp.Set(1)
		}
		var event repository.OutboxEvent
		if err := json.Unmarshal(message.Value, &event); err != nil || !validEvent(event) {
			slog.Error("invalid Kafka event", "error", err)
		} else {
			for ctx.Err() == nil {
				operationCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err = handle(operationCtx, event)
				cancel()
				if err == nil {
					if k.Metrics != nil {
						k.Metrics.KafkaConsumed.WithLabelValues("ok").Inc()
					}
					break
				}
				if k.Metrics != nil {
					k.Metrics.KafkaConsumed.WithLabelValues("error").Inc()
				}
				slog.Warn("Kafka event handling failed", "message_id", event.MessageID, "error", err)
				wait(ctx, time.Second)
			}
		}
		for ctx.Err() == nil {
			commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = k.reader.CommitMessages(commitCtx, message)
			cancel()
			if err == nil {
				break
			}
			slog.Warn("Kafka offset commit failed", "error", err)
			wait(ctx, time.Second)
		}
	}
}

func validEvent(event repository.OutboxEvent) bool {
	if event.MessageID <= 0 {
		return false
	}
	targets := 0
	for _, id := range []int64{event.RecipientID, event.GroupID, event.SenderID} {
		if id > 0 {
			targets++
		}
	}
	return targets == 1 && (event.SenderID == 0 && event.Status == "" || event.SenderID > 0 && event.Status == "delivered")
}

func RunPublisher(ctx context.Context, store *repository.Store, publish func(context.Context, repository.OutboxEvent) error) {
	for ctx.Err() == nil {
		operationCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		found, err := store.PublishNext(operationCtx, publish)
		cancel()
		if err != nil {
			slog.Warn("Kafka outbox publish failed", "error", err)
			wait(ctx, 2*time.Second)
		} else if !found {
			wait(ctx, 200*time.Millisecond)
		}
	}
}

func RunGroupPublisher(ctx context.Context, store *repository.Store, publish func(context.Context, repository.OutboxEvent) error) {
	for ctx.Err() == nil {
		operationCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		found, err := store.PublishNextGroup(operationCtx, publish)
		cancel()
		if err != nil {
			slog.Warn("Kafka group outbox publish failed", "error", err)
			wait(ctx, 2*time.Second)
		} else if !found {
			wait(ctx, 200*time.Millisecond)
		}
	}
}

func RunStatusPublisher(ctx context.Context, store *repository.Store, publish func(context.Context, repository.OutboxEvent) error) {
	for ctx.Err() == nil {
		operationCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		found, err := store.PublishNextStatus(operationCtx, publish)
		cancel()
		if err != nil {
			slog.Warn("Kafka status outbox publish failed", "error", err)
			wait(ctx, 2*time.Second)
		} else if !found {
			wait(ctx, 200*time.Millisecond)
		}
	}
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
