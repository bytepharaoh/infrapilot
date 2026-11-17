// Package kafkatest provisions disposable Apache Kafka for integration tests.
package kafkatest

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/bytepharaoh/infrapilot/pkg/kafka"
)

const image = "apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7"

type Topic struct {
	Name       string
	Partitions int32
	Config     map[string]string
}

type Broker struct {
	Container testcontainers.Container
	Address   string
}

// SetPaused suspends or resumes a disposable broker without affecting its advertised port or disks.
func (b Broker) SetPaused(ctx context.Context, paused bool) error {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("open Docker to control test broker: %w", err)
	}
	defer func() { _ = docker.Close() }()
	if paused {
		_, err = docker.ContainerPause(ctx, b.Container.GetContainerID(), client.ContainerPauseOptions{})
	} else {
		_, err = docker.ContainerUnpause(ctx, b.Container.GetContainerID(), client.ContainerUnpauseOptions{})
	}
	return err
}

// Start starts the pinned Kafka image and creates the supplied topics.
func Start(t testing.TB, topics ...Topic) Broker {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	request := testcontainers.ContainerRequest{
		Image: image, ExposedPorts: []string{"9092/tcp"},
		Entrypoint: []string{"/bin/bash", "-ec"},
		Cmd:        []string{"until [[ -x /tmp/infra-kafka-start ]]; do sleep 0.1; done; exec /tmp/infra-kafka-start"},
		Env: map[string]string{
			"CLUSTER_ID":                                     "PPAVlMrJQEW4p2J14OQO7A",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@127.0.0.1:29093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "INTERNAL",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "HOST:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_LISTENERS":                                "HOST://:9092,INTERNAL://:19092,CONTROLLER://:29093",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "3",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS":     "3",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_HEAP_OPTS":                                "-Xms256m -Xmx512m",
		},
		WaitingFor:     wait.ForLog("Kafka Server started").WithStartupTimeout(time.Minute),
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{PostStarts: []testcontainers.ContainerHook{installStarter}}},
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: request, Started: true})
	if ctr != nil {
		testcontainers.CleanupContainer(t, ctr)
	}
	require.NoError(t, err)
	address, err := brokerAddress(ctx, ctr)
	require.NoError(t, err)
	if len(topics) > 0 {
		require.NoError(t, createTopics(ctx, address, topics))
	}
	return Broker{Container: ctr, Address: address}
}

func installStarter(ctx context.Context, ctr testcontainers.Container) error {
	address, err := brokerAddress(ctx, ctr)
	if err != nil {
		return err
	}
	script := fmt.Sprintf("#!/bin/bash\nrm -f /tmp/infra-kafka-start\nexport KAFKA_ADVERTISED_LISTENERS=%q\nexec /etc/kafka/docker/run\n",
		"HOST://"+address+",INTERNAL://127.0.0.1:19092")
	return ctr.CopyToContainer(ctx, []byte(script), "/tmp/infra-kafka-start", 0o755)
}

func brokerAddress(ctx context.Context, ctr testcontainers.Container) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve test Kafka host: %w", err)
	}
	port, err := ctr.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return "", fmt.Errorf("resolve test Kafka port: %w", err)
	}
	return net.JoinHostPort(host, port.Port()), nil
}

func createTopics(ctx context.Context, address string, topics []Topic) error {
	client, err := kafka.NewClient(kafka.Config{Brokers: []string{address}, ClientID: "integration-admin"})
	if err != nil {
		return err
	}
	defer client.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 5000
	for _, topic := range topics {
		spec := kmsg.NewCreateTopicsRequestTopic()
		spec.Topic, spec.NumPartitions, spec.ReplicationFactor = topic.Name, topic.Partitions, 1
		for key, value := range topic.Config {
			spec.Configs = append(spec.Configs, kmsg.CreateTopicsRequestTopicConfig{Name: key, Value: &value})
		}
		request.Topics = append(request.Topics, spec)
	}
	result, err := request.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("create integration topics: %w", err)
	}
	for _, topic := range result.Topics {
		if topic.ErrorCode != 0 {
			return fmt.Errorf("create Kafka test topic %s: error code %d", topic.Topic, topic.ErrorCode)
		}
	}
	return nil
}
