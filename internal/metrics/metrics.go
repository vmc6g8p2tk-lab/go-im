package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry          *prometheus.Registry
	Messages          *prometheus.CounterVec
	MessageDuration   *prometheus.HistogramVec
	DeliveryEnqueue   *prometheus.CounterVec
	DeliveryConfirmed prometheus.Counter
	AckLatency        prometheus.Histogram
	Disconnects       prometheus.Counter
	SlowConsumers     prometheus.Counter
	DBDuration        *prometheus.HistogramVec
	RedisDuration     *prometheus.HistogramVec
	KafkaProducerUp   prometheus.Gauge
	KafkaConsumerUp   prometheus.Gauge
	KafkaPublished    *prometheus.CounterVec
	KafkaConsumed     *prometheus.CounterVec
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{
		registry:          r,
		Messages:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "im_messages_total", Help: "Message send attempts by kind and result."}, []string{"kind", "result"}),
		MessageDuration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "im_message_processing_seconds", Help: "Time to validate and persist a send request.", Buckets: prometheus.DefBuckets}, []string{"kind"}),
		DeliveryEnqueue:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "im_delivery_enqueue_total", Help: "Local WebSocket enqueue attempts; queued does not mean received."}, []string{"result"}),
		DeliveryConfirmed: prometheus.NewCounter(prometheus.CounterOpts{Name: "im_delivery_confirmed_total", Help: "First recipient ACKs persisted in PostgreSQL."}),
		AckLatency:        prometheus.NewHistogram(prometheus.HistogramOpts{Name: "im_ack_latency_seconds", Help: "Time from message creation to first recipient ACK.", Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60}}),
		Disconnects:       prometheus.NewCounter(prometheus.CounterOpts{Name: "im_websocket_disconnects_total", Help: "Authenticated WebSocket connections closed."}),
		SlowConsumers:     prometheus.NewCounter(prometheus.CounterOpts{Name: "im_slow_consumers_total", Help: "Connections closed because their bounded send queue filled."}),
		DBDuration:        prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "im_db_operation_seconds", Help: "PostgreSQL query duration; no raw SQL labels.", Buckets: prometheus.DefBuckets}, []string{"result"}),
		RedisDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "im_redis_operation_seconds", Help: "Redis operation duration.", Buckets: prometheus.DefBuckets}, []string{"operation", "result"}),
		KafkaProducerUp:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "im_kafka_producer_up", Help: "One after last successful Kafka publish, zero after failure."}),
		KafkaConsumerUp:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "im_kafka_consumer_up", Help: "One after last successful Kafka fetch, zero after failure."}),
		KafkaPublished:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "im_kafka_publish_total", Help: "Kafka publish attempts by result."}, []string{"result"}),
		KafkaConsumed:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "im_kafka_consume_total", Help: "Kafka event processing attempts by result."}, []string{"result"}),
	}
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.Messages, m.MessageDuration, m.DeliveryEnqueue, m.DeliveryConfirmed, m.AckLatency, m.Disconnects, m.SlowConsumers,
		m.DBDuration, m.RedisDuration, m.KafkaProducerUp, m.KafkaConsumerUp, m.KafkaPublished, m.KafkaConsumed)
	m.KafkaProducerUp.Set(0)
	m.KafkaConsumerUp.Set(0)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) RegisterHubGauges(connections, queueDepth func() float64) {
	m.registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "im_websocket_connections", Help: "Current authenticated WebSocket connections."}, connections),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "im_websocket_send_queue_frames", Help: "Frames currently buffered across local connection send queues."}, queueDepth))
}

func (m *Metrics) ObserveRedis(operation string, started time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.RedisDuration.WithLabelValues(operation, result).Observe(time.Since(started).Seconds())
}

type queryStartKey struct{}
type DBTracer struct{ Metrics *Metrics }

func (t DBTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, queryStartKey{}, time.Now())
}

func (t DBTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	started, ok := ctx.Value(queryStartKey{}).(time.Time)
	if !ok {
		return
	}
	result := "ok"
	if data.Err != nil {
		result = "error"
	}
	t.Metrics.DBDuration.WithLabelValues(result).Observe(time.Since(started).Seconds())
}
