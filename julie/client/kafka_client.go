package client

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"strings"
	"time"
)

type KafkaCluster struct {
	BootstrapServers []string
}

type Topic struct {
	Name              string
	ReplicationFactor int
	NumPartitions     int
	Config            map[string]*string
}

func NewKafkaCluster(bootstrapServers string) *KafkaCluster {
	return &KafkaCluster{BootstrapServers: []string{bootstrapServers}}
}

func newConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.ClientID = "terraform-provider-julieops"
	config.Admin.Timeout = time.Duration(60) * time.Second

	// if saslEnabled

	// if tlsIsEnabled
	return config, nil
}

func (k KafkaCluster) newAdminClient() (sarama.ClusterAdmin, error) {
	var config, err = newConfig()
	if err != nil {
		//TODO: Log the error
		return nil, err
	}

	adminClient, err := sarama.NewClusterAdmin(k.BootstrapServers, config)
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	return adminClient, nil
}

func (k *KafkaCluster) ListTopics(ctx context.Context, topic string) (topics []Topic, error error) {

	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	details, err := adminClient.ListTopics()
	if err != nil {
		log.Printf("[ERROR] Error retrieving topics from Kafka %s", k.BootstrapServers)
		return nil, err
	}

	var acc = make([]Topic, 0)
	for key, detail := range details {
		if strings.HasPrefix(key, topic) {
			config, err := retrieveTopicConfiguration(key, adminClient)
			log.Printf("DEBUG: ListTopics.add: topics = %d, topic = %s, numPartitions= %d", len(details), key, detail.NumPartitions)
			if err != nil {
				log.Printf("[ERROR] Error retrieving topics from Kafka %s", k.BootstrapServers)
				return nil, err
			}
			acc = append(acc, Topic{
				Name:              key,
				NumPartitions:     int(detail.NumPartitions),
				ReplicationFactor: int(detail.ReplicationFactor),
				Config:            config,
			})
		}
	}

	return acc, nil
}

func (k *KafkaCluster) DeleteTopic(ctx context.Context, topicName string) error {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = adminClient.DeleteTopic(topicName)
	if err != nil {
		return err
	}

	return nil
}

func retrieveTopicConfiguration(topic string, adminClient sarama.ClusterAdmin) (topicConfig map[string]*string, error error) {
	var config = make(map[string]*string, 10)

	resource := sarama.ConfigResource{Name: topic, Type: sarama.TopicResource}
	entries, err := adminClient.DescribeConfig(resource)
	if err != nil {
		log.Printf("[ERROR] while retrieving the topic configuration for topic %s", topic)
		return nil, err
	}

	for _, entry := range entries {
		if !isDefault(entry) {
			value := entry.Value
			config[entry.Name] = &value
		}
	}
	return config, nil
}

func isDefault(entry sarama.ConfigEntry) (isDefault bool) {
	return entry.Default || entry.Source == sarama.SourceDefault || entry.Source == sarama.SourceStaticBroker
}

func (k *KafkaCluster) CreateTopic(ctx context.Context,
	topicName string,
	numPartitions int,
	replicationFactor int,
	config map[string]*string) (topic *Topic, err error) {

	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var detail = sarama.TopicDetail{
		NumPartitions:     int32(numPartitions),
		ReplicationFactor: int16(replicationFactor),
		ConfigEntries:     config,
	}

	err = adminClient.CreateTopic(topicName, &detail, false)
	if err != nil && !isTopicAlreadyExistError(err) {
		log.Printf("[ERROR] Error creating a topic %s in Kafka %s", topicName, k.BootstrapServers)
		return nil, err
	}

	var resultTopic = Topic{
		Name:              topicName,
		ReplicationFactor: replicationFactor,
		NumPartitions:     numPartitions,
		Config:            config,
	}

	return &resultTopic, nil
}

func isTopicAlreadyExistError(err error) bool {
	return strings.HasPrefix(err.Error(), "kafka server: Topic with this name already exists")
}

func (k *KafkaCluster) UpdateTopic(ctx context.Context, name string, config map[string]*string) (err error) {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entries := make(map[string]*string, len(config))
	i := 0
	for k, v := range config {
		entries[k] = v
		i += 1
	}

	err = adminClient.AlterConfig(sarama.TopicResource, name, entries, false)
	return err
}
