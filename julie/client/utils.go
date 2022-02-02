package client

import (
	"github.com/Shopify/sarama"
	"hash/fnv"
	"log"
)

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

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
