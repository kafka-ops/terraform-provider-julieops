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
	BootstrapServers   []string
	Config             Config
	KafkaConnectClient KafkaConnectCluster
}

type Config struct {
	BootstrapServers []string
	IsSaslEnabled    bool
	SaslUsername     string
	SaslPassword     string
	SaslMechanism    string
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

type KafkaStreamsAcl struct {
	Id          string
	Project     string
	Principal   string
	ReadTopics  []string
	WriteTopics []string
	Metadata    map[string]string
}

func NewKafkaStreamsAcl(project string, principal string, readTopics []string, writeTopics []string, metadata map[string]string) *KafkaStreamsAcl {
	return &KafkaStreamsAcl{
		Id:          fmt.Sprintf("%s#%s", project, principal),
		Project:     project,
		Principal:   principal,
		ReadTopics:  readTopics,
		WriteTopics: writeTopics,
		Metadata:    metadata,
	}
}

type KafkaConnectAcl struct {
	Id                string
	Principal         string
	Group             string
	ReadTopics        []string
	WriteTopics       []string
	StatusTopic       string
	ConfigsTopic      string
	OffsetTopic       string
	EnableTopicCreate bool
	Metadata          map[string]string
}

func NewKafkaConnectAcl(principal string, group string, readTopics []string, writeTopics []string,
	statusTopic string, configsTopic string, offsetTopic string,
	enableTopicCreate bool, metadata map[string]string) *KafkaConnectAcl {

	return &KafkaConnectAcl{
		Id:                fmt.Sprintf("%s#%s", group, principal),
		Principal:         principal,
		Group:             group,
		ReadTopics:        readTopics,
		WriteTopics:       writeTopics,
		StatusTopic:       statusTopic,
		ConfigsTopic:      configsTopic,
		OffsetTopic:       offsetTopic,
		EnableTopicCreate: enableTopicCreate,
		Metadata:          metadata,
	}
}

type KafkaConnector struct {
	Name   string
	Config map[string]interface{}
}

func NewKafkaCluster(bootstrapServers string, config Config, kafkaConnectClient KafkaConnectCluster) *KafkaCluster {
	return &KafkaCluster{BootstrapServers: []string{bootstrapServers}, Config: config, KafkaConnectClient: kafkaConnectClient}
}

func (c *Config) newConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.ClientID = "terraform-provider-julieops"
	config.Admin.Timeout = time.Duration(60) * time.Second

	// if saslEnabled
	if c.IsSaslEnabled {
		switch c.SaslMechanism {
		case "scram-sha512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		case "scram-sha256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		case "plain":
		default:
			log.Fatalf("[ERROR] Invalid sasl mechanism \"%s\": can only be \"scram-sha256\", \"scram-sha512\" or \"plain\"", c.SaslMechanism)
		}
		config.Net.SASL.Enable = true
		config.Net.SASL.Password = c.SaslPassword
		config.Net.SASL.User = c.SaslUsername
		config.Net.SASL.Handshake = true
	} else {
		log.Printf("[WARN] SASL disabled username: '%s', password '%s'", c.SaslUsername, "****")
	}

	// if tlsIsEnabled
	return config, nil
}

func (k KafkaCluster) newAdminClient() (sarama.ClusterAdmin, error) {
	var config, err = k.Config.newConfig()
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

func (k KafkaCluster) IsAClusterAcl(acl sarama.ResourceAcls) bool {
	return acl.ResourceType == sarama.AclResourceCluster
}

func (k *KafkaCluster) ApplyAcls(resources AclResources) error {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return err
	}
	defer adminClient.Close()

	for _, resource := range resources.Resources {
		adminClient.CreateACL(resource.Resource, resource.Acl)
	}
	return nil
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

	var ops = []sarama.AclOperation{sarama.AclOperationDescribe, sarama.AclOperationRead}
	for op := range ops {
		var filter = sarama.AclFilter{
			ResourceName:              &consumerAcl.Project,
			ResourceType:              sarama.AclResourceTopic,
			Principal:                 &consumerAcl.Principal,
			Operation:                 sarama.AclOperation(op),
			PermissionType:            sarama.AclPermissionAllow,
			ResourcePatternTypeFilter: sarama.AclPatternPrefixed,
		}
		log.Printf("[DEBUG] Deleting ACL(s) for filter %o", filter)
		m, err := adminClient.DeleteACL(filter, false)
		log.Printf("[DEBUG] Deleted ACL(s) %d", len(m))
		if err != nil {
			log.Printf("[ERROR] Error deleting ACLs from Kafka %s", k.BootstrapServers)
			return err
		}
	}

	var filterGroup = sarama.AclFilter{
		ResourceName:              &consumerAcl.Group,
		ResourceType:              sarama.AclResourceGroup,
		Principal:                 &consumerAcl.Principal,
		Operation:                 sarama.AclOperationRead,
		PermissionType:            sarama.AclPermissionAllow,
		ResourcePatternTypeFilter: sarama.AclPatternLiteral,
	}
	log.Printf("[DEBUG] Deleting ACL(s) for filter %o", filterGroup)
	m, err := adminClient.DeleteACL(filterGroup, false)
	log.Printf("[DEBUG] Deleted ACL(s) %d", len(m))
	if err != nil {
		log.Printf("[ERROR] Error deleting ACLs from Kafka %s", k.BootstrapServers)
		return err
	}
	return nil
}

func (k *KafkaCluster) CreateKafkaStreamsAcl(kStreamsAcl KafkaStreamsAcl) (*KafkaStreamsAcl, error) {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	defer adminClient.Close()

	resources, acls := createTopicAcls(kStreamsAcl.ReadTopics, kStreamsAcl.Principal, sarama.AclOperationRead)
	for i, resource := range resources {
		adminClient.CreateACL(resource, acls[i])
	}

	resources, acls = createTopicAcls(kStreamsAcl.WriteTopics, kStreamsAcl.Principal, sarama.AclOperationWrite)
	for i, resource := range resources {
		adminClient.CreateACL(resource, acls[i])
	}

	resource, acl := createKStreamAcl(kStreamsAcl.Project, kStreamsAcl.Principal, sarama.AclResourceTopic, sarama.AclOperationAll)
	adminClient.CreateACL(resource, acl)
	resource, acl = createKStreamAcl(kStreamsAcl.Project, kStreamsAcl.Principal, sarama.AclResourceGroup, sarama.AclOperationRead)
	adminClient.CreateACL(resource, acl)

	return &kStreamsAcl, nil
}

func (k *KafkaCluster) CreateKafkaConnectAcl(kConnectAcl KafkaConnectAcl, b KafkaAclsBuilder) (*KafkaConnectAcl, error) {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return nil, err
	}
	defer adminClient.Close()

	resources, err := b.KafkaConnectAclsBuilder(kConnectAcl)
	if err != nil {
		return nil, err
	}
	for _, resource := range resources.Resources {
		adminClient.CreateACL(resource.Resource, resource.Acl)
	}
	return &kConnectAcl, nil
}

func (k *KafkaCluster) DeleteKafkaConnectAcl(kConnectAcl KafkaConnectAcl, b KafkaAclsBuilder) error {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return err
	}
	defer adminClient.Close()

	resources, err := b.KafkaConnectAclsBuilder(kConnectAcl)
	if err != nil {
		return err
	}
	for _, resource := range resources.Resources {

		var filter = sarama.AclFilter{
			ResourceName:              &resource.Resource.ResourceName,
			ResourceType:              resource.Resource.ResourceType,
			Principal:                 &resource.Acl.Principal,
			Operation:                 resource.Acl.Operation,
			PermissionType:            resource.Acl.PermissionType,
			ResourcePatternTypeFilter: resource.Resource.ResourcePatternType,
		}
		log.Printf("[DEBUG] Deleting ACL(s) for filter %o", filter)
		m, err := adminClient.DeleteACL(filter, false)
		log.Printf("[DEBUG] Deleted ACL(s) %d", len(m))
		if err != nil {
			log.Printf("[ERROR] Error deleting ACLs from Kafka %s", k.BootstrapServers)
			return err
		}
	}

	return nil
}

func createKStreamAcl(project string, principal string, resourceType sarama.AclResourceType, op sarama.AclOperation) (sarama.Resource, sarama.Acl) {
	resource := sarama.Resource{
		ResourceName:        project,
		ResourceType:        resourceType,
		ResourcePatternType: sarama.AclPatternPrefixed,
	}

	acl := sarama.Acl{
		Principal:      principal,
		Host:           "*",
		Operation:      op,
		PermissionType: sarama.AclPermissionAllow,
	}
	return resource, acl
}

func createTopicAcls(topics []string, principal string, op sarama.AclOperation) ([]sarama.Resource, []sarama.Acl) {
	var resources = make([]sarama.Resource, len(topics))
	var acls = make([]sarama.Acl, len(topics))

	for i, topic := range topics {

		resources[i] = sarama.Resource{
			ResourceName:        topic,
			ResourceType:        sarama.AclResourceTopic,
			ResourcePatternType: sarama.AclPatternLiteral,
		}

		acls[i] = sarama.Acl{
			Principal:      principal,
			Host:           "*",
			Operation:      op,
			PermissionType: sarama.AclPermissionAllow,
		}
	}
	return resources, acls
}

func (k *KafkaCluster) DeleteKafkaStreamsAcl(kStreamsAcl KafkaStreamsAcl) error {
	adminClient, err := k.newAdminClient()
	if err != nil {
		log.Printf("[ERROR] Error connecting to Kafka %s", k.BootstrapServers)
		return err
	}
	defer adminClient.Close()

	resources, acls := createTopicAcls(kStreamsAcl.ReadTopics, kStreamsAcl.Principal, sarama.AclOperationRead)
	for i, resource := range resources {
		deleteAcl(adminClient, resource, acls[i])
	}

	resources, acls = createTopicAcls(kStreamsAcl.WriteTopics, kStreamsAcl.Principal, sarama.AclOperationWrite)
	for i, resource := range resources {
		deleteAcl(adminClient, resource, acls[i])
	}

	resource, acl := createKStreamAcl(kStreamsAcl.Project, kStreamsAcl.Principal, sarama.AclResourceTopic, sarama.AclOperationAll)
	deleteAcl(adminClient, resource, acl)
	resource, acl = createKStreamAcl(kStreamsAcl.Project, kStreamsAcl.Principal, sarama.AclResourceGroup, sarama.AclOperationRead)
	deleteAcl(adminClient, resource, acl)

	return nil
}

func deleteAcl(adminClient sarama.ClusterAdmin, resource sarama.Resource, acl sarama.Acl) error {
	var filter = sarama.AclFilter{
		ResourceName:              &resource.ResourceName,
		ResourceType:              resource.ResourceType,
		Principal:                 &acl.Principal,
		Operation:                 acl.Operation,
		PermissionType:            acl.PermissionType,
		ResourcePatternTypeFilter: resource.ResourcePatternType,
	}
	log.Printf("[DEBUG] Deleting ACL(s) for filter %o", filter)
	m, err := adminClient.DeleteACL(filter, false)
	log.Printf("[DEBUG] Deleted ACL(s) %d", len(m))
	if err != nil {
		log.Printf("[ERROR] Error deleting ACLs %s", err.Error())
		return err
	}
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
