package client

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type KafkaCluster struct {
	BootstrapServers string
}

type Topic struct {
	Name              string
	ReplicationFactor int
	NumPartitions     int
	Config            map[string]string
}

func NewKafkaCluster(bootstrapServers string) *KafkaCluster {
	return &KafkaCluster{BootstrapServers: bootstrapServers}
}

func (k *KafkaCluster) ListTopics(ctx context.Context, topic *string, allTopics bool) (topics []Topic, err error) {

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": k.BootstrapServers})
	if err != nil {
		// LOG the error
		return nil, err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metadata, err := adminClient.GetMetadata(topic, allTopics, 60000)

	if err != nil {
		// LOG the error
		return nil, err
	}

	var acc = make([]Topic, len(metadata.Topics))

	for _, val := range metadata.Topics {
		log.Printf("DEBUG: ListTopics: topics = %d, topic = %s, numPartitions= %d", len(metadata.Topics), val.Topic, len(val.Partitions))
		acc = append(acc, Topic{
			Name:              val.Topic,
			NumPartitions:     len(val.Partitions),
			ReplicationFactor: doReplicationFactor(val.Partitions[0]),
			Config:            retrieveTopicConfiguration(ctx, val.Topic, adminClient),
		})
	}

	return acc, nil
}

func (k *KafkaCluster) DeleteTopic(ctx context.Context, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": k.BootstrapServers})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topics := []string{topicName}

	_, err = adminClient.DeleteTopics(ctx, topics)
	if err != nil {
		return err
	}
	return nil
}

func retrieveTopicConfiguration(ctx context.Context, topic string, adminClient *kafka.AdminClient) (topicConfig map[string]string) {
	var config = make(map[string]string, 10)

	resourceType, _ := kafka.ResourceTypeFromString("topic")
	results, _ := adminClient.DescribeConfigs(ctx, []kafka.ConfigResource{{Type: resourceType, Name: topic}})

	for _, result := range results {
		for _, entry := range result.Config {
			if !isDefault(entry) {
				config[entry.Name] = entry.Value
			}
		}
	}

	return config
}

func isDefault(entry kafka.ConfigEntryResult) (isDefault bool) {
	return entry.Source == kafka.ConfigSourceDefault || entry.Source == kafka.ConfigSourceDynamicDefaultBroker
}

func doReplicationFactor(partitions kafka.PartitionMetadata) (count int) {
	return len(partitions.Replicas)
}

func (k *KafkaCluster) CreateTopic(ctx context.Context, topicName string, numPartitions int, replicationFactor int, config map[string]string) (topic *Topic, err error) {

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": k.BootstrapServers})
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
			Topic:             topicName,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			Config:            config}},
		kafka.SetAdminOperationTimeout(timeOut))

	if err != nil {
		//LOG the error
		return nil, err
	}

	var acc = make([]Topic, len(results))

	for _, result := range results {
		acc = append(acc, Topic{Name: result.Topic})
	}

	var resultTopic = Topic{
		Name:              results[0].Topic,
		ReplicationFactor: replicationFactor,
		NumPartitions:     numPartitions,
		Config:            config,
	}

	return &resultTopic, nil
}
