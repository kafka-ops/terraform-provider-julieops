package client

import (
	"context"
	"fmt"
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

type ConsumerAcl struct {
	Id        string
	Project   string
	Principal string
	Group     string
	Metadata  map[string]string
}

func NewConsumerAcl(project string, principal string, group string, metadata map[string]string) *ConsumerAcl {
	return &ConsumerAcl{
		Id:        fmt.Sprintf("%s#%s#%s", project, principal, group),
		Project:   project,
		Principal: principal,
		Group:     group,
		Metadata:  metadata,
	}
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

func (k KafkaCluster) IsAGroupAcl(acl sarama.ResourceAcls) bool {
	return acl.ResourceType == sarama.AclResourceGroup
}

func (k KafkaCluster) IsATopicAcl(acl sarama.ResourceAcls) bool {
	return acl.ResourceType == sarama.AclResourceTopic
}

func (k *KafkaCluster) CreateConsumerAcl(consumerAcl ConsumerAcl) (*ConsumerAcl, error) {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	defer adminClient.Close()

	resource := sarama.Resource{
		ResourceName:        consumerAcl.Project,
		ResourceType:        sarama.AclResourceTopic,
		ResourcePatternType: sarama.AclPatternPrefixed,
	}

	operations := []sarama.AclOperation{sarama.AclOperationDescribe, sarama.AclOperationRead}

	for _, operation := range operations {
		acl := sarama.Acl{
			Principal:      consumerAcl.Principal,
			Host:           "*",
			Operation:      operation,
			PermissionType: sarama.AclPermissionAllow,
		}
		adminClient.CreateACL(resource, acl)
	}

	resource = sarama.Resource{
		ResourceName:        consumerAcl.Group,
		ResourceType:        sarama.AclResourceGroup,
		ResourcePatternType: sarama.AclPatternLiteral,
	}

	acl := sarama.Acl{
		Principal:      consumerAcl.Principal,
		Host:           "*",
		Operation:      sarama.AclOperationRead,
		PermissionType: sarama.AclPermissionAllow,
	}

	adminClient.CreateACL(resource, acl)

	return &consumerAcl, nil
}

func (k *KafkaCluster) DeleteConsumerAcl(consumerAcl ConsumerAcl) error {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return err
	}
	defer adminClient.Close()

	var filter = sarama.AclFilter{
		ResourceName: &consumerAcl.Project,
		ResourceType: sarama.AclResourceTopic,
		Principal:    &consumerAcl.Principal,
	}
	adminClient.DeleteACL(filter, false)

	var filterGroup = sarama.AclFilter{
		ResourceName: &consumerAcl.Group,
		ResourceType: sarama.AclResourceGroup,
		Principal:    &consumerAcl.Principal,
	}
	adminClient.DeleteACL(filterGroup, false)

	return nil
}

func (k KafkaCluster) ListAcls(principal string) ([]sarama.ResourceAcls, error) {

	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	defer adminClient.Close()

	filter := sarama.AclFilter{
		ResourceType: sarama.AclResourceAny,
		Principal:    &principal,
	}

	return adminClient.ListAcls(filter)
}
