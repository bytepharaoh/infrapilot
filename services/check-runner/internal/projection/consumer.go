// Package projection replays the compacted control-plane resource lifecycle topic.
package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	eventsv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/events/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store/db"
)

const (
	topic          = "resource.state.v1"
	partitionCount = 12
)

// Consumer uses broker positions and a durable PostgreSQL checkpoint for each
// partition. Concurrent replicas can replay the same range without double effects.
type Consumer struct {
	store  *store.Store
	config kafka.Config
	admin  *kgo.Client
	logger *slog.Logger
}

// New owns no network resources beyond the supplied application client.
func New(st *store.Store, config kafka.Config, admin *kgo.Client, logger *slog.Logger) *Consumer {
	return &Consumer{store: st, config: config, admin: admin, logger: logger}
}

// Run continuously catches up to read-committed samples and backs off on errors.
func (c *Consumer) Run(ctx context.Context) error {
	var delay time.Duration
	for ctx.Err() == nil {
		if err := c.store.Project(ctx, c.sample); err != nil && ctx.Err() == nil {
			c.logger.WarnContext(ctx, "resource projection catch-up failed", slog.Any("error", err))
			delay = min(10*time.Second, max(time.Second, delay*2))
		} else {
			delay = time.Second
		}
		select {
		case <-ctx.Done():
		case <-time.After(delay + time.Duration(rand.IntN(500))*time.Millisecond):
		}
	}
	return nil
}

func (c *Consumer) sample(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()
	// Metadata and offsets are sampled anew so a topic replacement cannot be
	// mistaken for progress, even if its offset range overlaps the old topic.
	id, err := c.topicID(ctx)
	if err != nil {
		return err
	}
	first, err := c.offsets(ctx, -2)
	if err != nil {
		return err
	}
	sampled := time.Now().UTC()
	end, err := c.offsets(ctx, -1)
	if err != nil {
		return err
	}
	states := make(map[int32]db.ResourcePartition, partitionCount)
	start := make(map[int32]kgo.Offset, partitionCount)
	for part := range int32(partitionCount) {
		state, prepareErr := c.store.PreparePartition(ctx, part, uuid.UUID(id).String(), first[part], end[part])
		if prepareErr != nil {
			return prepareErr
		}
		if state.NextOffset >= end[part] {
			if confirmErr := c.store.ConfirmProjection(ctx, state, end[part], sampled); confirmErr != nil {
				return confirmErr
			}
			continue
		}
		states[part] = state
		start[part] = kgo.NewOffset().At(state.NextOffset)
	}
	if len(start) == 0 {
		return nil
	}
	client, err := kafka.NewClient(c.config, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: start}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.KeepControlRecords(), kgo.FetchMaxWait(250*time.Millisecond))
	if err != nil {
		return err
	}
	defer client.Close()
	for len(states) > 0 && ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 128)
		for _, fetchErr := range fetches.Errors() {
			if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch resource partition %d: %w", fetchErr.Partition, fetchErr.Err)
		}
		for _, fetch := range fetches {
			for _, fetchedTopic := range fetch.Topics {
				if fetchedTopic.Topic != topic || fetchedTopic.TopicID != id {
					return errors.New("resource topic identity changed during replay")
				}
				for _, partition := range fetchedTopic.Partitions {
					state, ok := states[partition.Partition]
					if !ok {
						continue
					}
					for _, record := range partition.Records {
						if applyErr := c.applyRecord(ctx, state, record); applyErr != nil {
							return applyErr
						}
						state.NextOffset = record.Offset + 1
					}
					if state.NextOffset >= end[partition.Partition] {
						if confirmErr := c.store.ConfirmProjection(ctx, state, end[partition.Partition], sampled); confirmErr != nil {
							return confirmErr
						}
						delete(states, partition.Partition)
						client.PauseFetchPartitions(map[string][]int32{topic: {partition.Partition}})
					} else {
						states[partition.Partition] = state
					}
				}
			}
		}
	}
	return nil
}

func (c *Consumer) applyRecord(ctx context.Context, state db.ResourcePartition, record *kgo.Record) error {
	if record.Attrs.IsControl() {
		return c.store.ApplyProjection(ctx, state, record.Offset, nil)
	}
	event, change, err := decode(record)
	if err != nil {
		holdErr := c.store.HoldProjection(ctx, state, record.Offset)
		return fmt.Errorf("invalid lifecycle event on partition %d offset %d: %w", state.KafkaPartition, record.Offset, errors.Join(err, holdErr))
	}
	return c.store.ApplyProjection(envelope.ContextWithTrace(ctx, event), state, record.Offset, &change)
}

func decode(record *kgo.Record) (*eventsv1.Envelope, store.ProjectedChange, error) {
	value := record.Value
	if value == nil {
		for _, header := range record.Headers {
			if header.Key == "infrapilot-envelope" {
				if value != nil {
					return nil, store.ProjectedChange{}, errors.New("duplicate tombstone envelope")
				}
				value = header.Value
			}
		}
	}
	var event eventsv1.Envelope
	if err := proto.Unmarshal(value, &event); err != nil {
		return nil, store.ProjectedChange{}, errors.New("invalid lifecycle envelope")
	}
	if err := envelope.Validate(&event); err != nil || event.GetProducer() != "control-plane" || event.GetEventType() != "controlplane.resource.changed" || event.GetSchemaVersion() != 1 {
		return nil, store.ProjectedChange{}, errors.New("unexpected lifecycle envelope")
	}
	var payload cpv1.ResourceStateChanged
	if err := envelope.Unpack(&event, &payload); err != nil {
		return nil, store.ProjectedChange{}, errors.New("invalid lifecycle payload")
	}
	org, orgErr := uuid.Parse(event.GetOrganizationId())
	res, resErr := uuid.Parse(event.GetSubjectId())
	project, projectErr := uuid.Parse(payload.GetProjectId())
	if orgErr != nil || resErr != nil || projectErr != nil || org == uuid.Nil || res == uuid.Nil || project == uuid.Nil ||
		res.String() != event.GetSubjectId() || project.String() != payload.GetProjectId() ||
		string(record.Key) != org.String()+"/"+res.String() || payload.GetRevision() == 0 || payload.GetRevision() > math.MaxInt64 {
		return nil, store.ProjectedChange{}, errors.New("invalid lifecycle identity or revision")
	}
	change := store.ProjectedChange{Organization: org, Resource: res, Project: project, Revision: int64(payload.GetRevision())}
	switch payload.GetStatus() {
	case cpv1.ResourceStateStatus_RESOURCE_STATE_STATUS_ACTIVE:
		change.Active = true
	case cpv1.ResourceStateStatus_RESOURCE_STATE_STATUS_INACTIVE:
	case cpv1.ResourceStateStatus_RESOURCE_STATE_STATUS_PURGED:
		change.Removed = true
	default:
		return nil, store.ProjectedChange{}, errors.New("invalid resource status")
	}
	if (record.Value == nil) != change.Removed {
		return nil, store.ProjectedChange{}, errors.New("lifecycle status does not match the Kafka value")
	}
	return &event, change, nil
}

func (c *Consumer) topicID(ctx context.Context) ([16]byte, error) {
	name := topic
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	response, err := request.RequestWith(ctx, c.admin)
	if err != nil {
		return [16]byte{}, fmt.Errorf("describe resource topic: %w", err)
	}
	if len(response.Topics) != 1 {
		return [16]byte{}, errors.New("resource topic metadata is missing")
	}
	meta := response.Topics[0]
	if err := kerr.ErrorForCode(meta.ErrorCode); err != nil {
		return [16]byte{}, fmt.Errorf("describe resource topic: %w", err)
	}
	if len(meta.Partitions) != partitionCount || meta.TopicID == ([16]byte{}) {
		return [16]byte{}, errors.New("resource topic has incompatible partitions or identity")
	}
	return meta.TopicID, nil
}

func (c *Consumer) offsets(ctx context.Context, stamp int64) ([partitionCount]int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	request.IsolationLevel = 1 // READ_COMMITTED; latest is the last stable offset.
	selection := kmsg.ListOffsetsRequestTopic{Topic: topic, Partitions: make([]kmsg.ListOffsetsRequestTopicPartition, 0, partitionCount)}
	for partition := range int32(partitionCount) {
		selection.Partitions = append(selection.Partitions, kmsg.ListOffsetsRequestTopicPartition{Partition: partition, CurrentLeaderEpoch: -1, Timestamp: stamp})
	}
	request.Topics = []kmsg.ListOffsetsRequestTopic{selection}
	var offsets [partitionCount]int64
	var found [partitionCount]bool
	for _, shard := range c.admin.RequestSharded(ctx, request) {
		if shard.Err != nil {
			return offsets, fmt.Errorf("sample resource offsets: %w", shard.Err)
		}
		response, ok := shard.Resp.(*kmsg.ListOffsetsResponse)
		if !ok {
			return offsets, errors.New("unexpected resource offset response")
		}
		for _, selection := range response.Topics {
			for _, entry := range selection.Partitions {
				if err := kerr.ErrorForCode(entry.ErrorCode); err != nil {
					return offsets, fmt.Errorf("sample resource offset for partition %d: %w", entry.Partition, err)
				}
				if entry.Partition < 0 || entry.Partition >= partitionCount || entry.Offset < 0 {
					return offsets, errors.New("invalid resource offset sample")
				}
				offsets[entry.Partition], found[entry.Partition] = entry.Offset, true
			}
		}
	}
	for _, ok := range found {
		if !ok {
			return offsets, errors.New("resource offset sample missing partition")
		}
	}
	return offsets, nil
}
