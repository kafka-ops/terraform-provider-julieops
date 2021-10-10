package client

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type KafkaCluster struct {
	bootstrapServers string
}

type Topic struct {
	name string
	replicationFactor int
	numPartitions int
	config map[string]string
}

func NewKafkaCluster() *KafkaCluster {
	return &KafkaCluster{}
}

func (k *KafkaCluster) ListTopics(ctx context.Context) (topics []Topic, err error) {

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": k.bootstrapServers})
	if err != nil {
		// LOG the error
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metadata, err := adminClient.GetMetadata(nil, true, 60000)

	if err != nil {
		// LOG the error
		return nil, err
	}

	var acc = make([]Topic, len(metadata.Topics))

	for _,val := range metadata.Topics {
		acc = append(acc, Topic{
			name: val.Topic,
			numPartitions: len(val.Partitions),
		})
	}

	return acc, nil

}

func (k *KafkaCluster) CreateTopic(ctx context.Context, topic string, numPartitions int, replicationFactor int) (topics []Topic, err error) {

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": k.bootstrapServers})
	if err != nil {
		// LOG the error
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeOut, _ := time.ParseDuration("60s")

	results, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic: topic,
			NumPartitions: numPartitions,
			ReplicationFactor: replicationFactor}},
		kafka.SetAdminOperationTimeout(timeOut))

	if err != nil {
		//LOG the error
		return nil, err
	}

	var acc = make([]Topic, len(results))

	for _, result := range results {
		acc = append(acc, Topic{name: result.Topic})
	}

	return acc, nil
}