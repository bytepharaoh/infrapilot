// Package kafkaconsumer commits offsets after durable stream DB transactions.
package kafkaconsumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/service"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store"
)

const attemptWindow = 15 * time.Second

// Consumer is one process's member of the shared stream processor consumer group.
type Consumer struct {
	config           kafka.Config
	group            string
	producer         *kgo.Client
	writer           store.Writer
	tracer           trace.Tracer
	logger           *slog.Logger
	lag, paused      metric.Int64Gauge
	dlq, expirations metric.Int64Counter
	stop, done       chan struct{}
	stopOnce         sync.Once
}

func New(cfg kafka.Config, group string, producer *kgo.Client, writer store.Writer, meter metric.Meter, tracer trace.Tracer, logger *slog.Logger) (*Consumer, error) {
	lag, lagErr := meter.Int64Gauge("stream_processor_consumer_lag", metric.WithDescription("Read-committed records beyond the durable consumer offset"))
	paused, pausedErr := meter.Int64Gauge("stream_processor_consumer_partition_paused")
	dlq, dlqErr := meter.Int64Counter("stream_processor_consumer_dlq")
	expirations, expirationErr := meter.Int64Counter("stream_processor_consumer_expired")
	if err := errors.Join(lagErr, pausedErr, dlqErr, expirationErr); err != nil {
		return nil, fmt.Errorf("register consumer metrics: %w", err)
	}
	return &Consumer{config: cfg, group: group, producer: producer, writer: writer, tracer: tracer, logger: logger,
		lag: lag, paused: paused, dlq: dlq, expirations: expirations, stop: make(chan struct{}), done: make(chan struct{})}, nil
}

func (c *Consumer) Name() string { return "stream-kafka-consumer" }

func (c *Consumer) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stop) })
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain Kafka consumer: %w", ctx.Err())
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer close(c.done)
	for {
		if c.stopping(ctx) {
			return nil
		}
		client, err := kafka.NewClient(c.config,
			kgo.ConsumerGroup(c.group), kgo.ConsumeTopics(service.ChecksTopic, service.MetricsTopic),
			kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			return fmt.Errorf("create stream consumer: %w", err)
		}
		err = c.consume(ctx, client)
		client.CloseAllowingRebalance()
		if c.stopping(ctx) {
			return nil
		}
		if err != nil {
			c.logger.WarnContext(ctx, "Kafka stream paused after processing failure", slog.Any("error", err))
		}
		timer := time.NewTimer(time.Second + rand.N(500*time.Millisecond))
		select {
		case <-ctx.Done():
		case <-c.stop:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (c *Consumer) stopping(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-c.stop:
		return true
	default:
		return false
	}
}

func (c *Consumer) consume(ctx context.Context, client *kgo.Client) error {
	for !c.stopping(ctx) {
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := client.PollRecords(pollCtx, service.MaxBatchRecords)
		cancel()
		if fetches.IsClientClosed() {
			return errors.New("stream consumer closed")
		}
		var err error
		deadline, finish := context.WithTimeout(ctx, attemptWindow)
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if err == nil && len(partition.Records) != 0 {
				err = c.processPartition(deadline, client, partition)
			}
		})
		finish()
		// No commit is sent after this point until a fresh poll grants ownership.
		client.AllowRebalance()
		if err != nil {
			return err
		}
		for _, fetchErr := range fetches.Errors() {
			if !errors.Is(fetchErr.Err, context.DeadlineExceeded) && !errors.Is(fetchErr.Err, context.Canceled) {
				return fmt.Errorf("poll Kafka partition: %w", fetchErr.Err)
			}
		}
	}
	return nil
}

func (c *Consumer) processPartition(ctx context.Context, client *kgo.Client, p kgo.FetchTopicPartition) error {
	attrs := metric.WithAttributes(attribute.String("topic", p.Topic), attribute.Int("partition", int(p.Partition)))
	last := p.Records[len(p.Records)-1]
	c.lag.Record(ctx, max(0, p.LastStableOffset-p.Records[0].Offset), attrs)
	paused := map[string][]int32{p.Topic: {p.Partition}}
	client.PauseFetchPartitions(paused)
	c.paused.Record(ctx, 1, attrs)
	defer func() {
		client.ResumeFetchPartitions(paused)
		c.paused.Record(ctx, 0, attrs)
	}()
	delay := 200 * time.Millisecond
	for {
		work, cancel := context.WithTimeout(ctx, 4*time.Second)
		err := c.apply(work, p.Topic, p.Records)
		cancel()
		if err == nil {
			// CommitRecords stores last.Offset+1 and blocks until the broker replies.
			if commitErr := client.CommitRecords(ctx, last); commitErr != nil {
				return fmt.Errorf("commit durable stream offset: %w", commitErr)
			}
			c.lag.Record(ctx, max(0, p.LastStableOffset-last.Offset-1), attrs)
			return nil
		}
		timer := time.NewTimer(delay + rand.N(delay/2))
		select {
		case <-timer.C:
			delay = min(2*delay, time.Second)
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("persist stream partition before retry deadline: %w", errors.Join(ctx.Err(), err))
		}
	}
}

func (c *Consumer) apply(ctx context.Context, topic string, records []*kgo.Record) error {
	var checks []domain.Check
	var metrics []domain.Metric
	var org uuid.UUID
	flush := func() error {
		if len(checks) > 0 {
			if err := c.writer.Checks(ctx, org, checks); err != nil {
				return err
			}
		}
		if len(metrics) > 0 {
			if err := c.writer.Metrics(ctx, org, metrics); err != nil {
				return err
			}
		}
		checks = checks[:0]
		metrics = metrics[:0]
		return nil
	}

	now := time.Now()
	for _, record := range records {
		env, err := service.DecodeEnvelope(record.Value)
		next := uuid.Nil
		if err == nil {
			next, err = domain.ParseID(env.GetOrganizationId(), "organization_id")
		}
		if next != org {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			org = next
		}
		if err == nil {
			switch topic {
			case service.ChecksTopic:
				check, decodeErr := service.DecodeCheck(env, string(record.Key), now)
				err = decodeErr
				if err == nil {
					checks = append(checks, check)
				}
			case service.MetricsTopic:
				sample, decodeErr := service.DecodeMetric(env, string(record.Key), now)
				err = decodeErr
				if err == nil {
					metrics = append(metrics, sample)
				}
			default:
				err = errors.New("unexpected stream topic")
			}
		}
		if errors.Is(err, service.ErrExpired) {
			c.expirations.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
			continue
		}
		if err != nil {
			if dlqErr := c.deadLetter(ctx, record); dlqErr != nil {
				return dlqErr
			}
		}
	}
	if len(records) != 0 {
		if env, err := service.DecodeEnvelope(records[0].Value); err == nil {
			ctx = envelope.ContextWithTrace(ctx, env)
		}
	}
	ctx, span := c.tracer.Start(ctx, "apply stream batch", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	return flush()
}

func (c *Consumer) deadLetter(ctx context.Context, source *kgo.Record) error {
	payload := source.Value
	if _, err := service.DecodeEnvelope(payload); err != nil {
		fingerprint := sha256.Sum256(payload)
		env, wrapErr := envelope.New(ctx, envelope.Params{
			Type: "stream.record.rejected", SchemaVersion: 1, SubjectID: hex.EncodeToString(fingerprint[:]),
			Producer: "stream-processor", Payload: wrapperspb.Bytes(payload[:min(len(payload), 512<<10)]),
		})
		if wrapErr != nil {
			return fmt.Errorf("prepare dead-letter envelope: %w", wrapErr)
		}
		payload, wrapErr = proto.Marshal(env)
		if wrapErr != nil {
			return fmt.Errorf("encode dead-letter envelope: %w", wrapErr)
		}
	}
	key := source.Topic + "/" + strconv.FormatInt(int64(source.Partition), 10) + "/" + strconv.FormatInt(source.Offset, 10)
	record := &kgo.Record{Topic: source.Topic + ".dlq", Key: []byte(key), Value: payload,
		Headers: []kgo.RecordHeader{{Key: "source_topic", Value: []byte(source.Topic)},
			{Key: "source_partition", Value: []byte(strconv.FormatInt(int64(source.Partition), 10))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(source.Offset, 10))},
			{Key: "error_class", Value: []byte("invalid_stream_record")}}}
	if err := kafka.Publish(ctx, c.producer, record); err != nil {
		return fmt.Errorf("acknowledge Kafka dead letter: %w", err)
	}
	c.dlq.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", source.Topic)))
	return nil
}
