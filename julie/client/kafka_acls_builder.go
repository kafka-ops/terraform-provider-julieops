package client

import (
	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type KafkaAclsBuilder struct {
	Client *KafkaCluster
}

type AclResources struct {
	Resources []AclResourceInfo
}

type AclResourceInfo struct {
	Resource sarama.Resource
	Acl      sarama.Acl
}

type Convert func(d *schema.ResourceData) interface{}

type AclBuilder func(acl interface{}) (AclResources, error)

func (b KafkaAclsBuilder) BuildAcls(d *schema.ResourceData, fnConvert Convert, fnBuilder AclBuilder) (interface{}, AclResources, error) {
	acl := fnConvert(d)
	resources, err := fnBuilder(acl)
	return acl, resources, err
}

func (b KafkaAclsBuilder) ConsumerAclsBuilder(aclInterface interface{}) (AclResources, error) {
	resources := make([]AclResourceInfo, 3)
	consumerAcl := aclInterface.(ConsumerAcl)

	resource := sarama.Resource{
		ResourceName:        consumerAcl.Project,
		ResourceType:        sarama.AclResourceTopic,
		ResourcePatternType: sarama.AclPatternPrefixed,
	}

	operations := []sarama.AclOperation{sarama.AclOperationDescribe, sarama.AclOperationRead}
	i := 0
	for _, operation := range operations {
		acl := sarama.Acl{
			Principal:      consumerAcl.Principal,
			Host:           "*",
			Operation:      operation,
			PermissionType: sarama.AclPermissionAllow,
		}
		resources[i] = AclResourceInfo{Resource: resource, Acl: acl}
		i = i + 1
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
	resources[2] = AclResourceInfo{Resource: resource, Acl: acl}

	return AclResources{Resources: resources}, nil
}

func (b KafkaAclsBuilder) KafkaStreamsAclsBuilder(aclInterface interface{}) (AclResources, error) {
	kStreamsAcl := aclInterface.(KafkaStreamsAcl)
	resourceInfos := make([]AclResourceInfo, len(kStreamsAcl.ReadTopics)+len(kStreamsAcl.WriteTopics)+2)

	i := 0
	resources, acls := createTopicAcls(kStreamsAcl.ReadTopics, kStreamsAcl.Principal, sarama.AclOperationRead)
	for j, resource := range resources {
		resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acls[j]}
		i = i + 1
	}

	resources, acls = createTopicAcls(kStreamsAcl.WriteTopics, kStreamsAcl.Principal, sarama.AclOperationWrite)
	for j, resource := range resources {
		resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acls[j]}
		i = i + 1
	}

	resource, acl := createKStreamAcl(kStreamsAcl.Project, kStreamsAcl.Principal, sarama.AclResourceTopic, sarama.AclOperationAll)
	resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acl}
	resource, acl = createKStreamAcl(kStreamsAcl.Project, kStreamsAcl.Principal, sarama.AclResourceGroup, sarama.AclOperationRead)
	resourceInfos[i+1] = AclResourceInfo{Resource: resource, Acl: acl}
	return AclResources{Resources: resourceInfos}, nil
}

func (b KafkaAclsBuilder) KafkaConnectAclsBuilder(aclInterface interface{}) (AclResources, error) {
	kafkaConnectAcl := aclInterface.(KafkaConnectAcl)
	numOfResourcesToBuild := GetNumberOfResourcesForKafkaConnect(kafkaConnectAcl)
	resourceInfos := make([]AclResourceInfo, numOfResourcesToBuild)

	i := 0
	resources, acls := createTopicAcls(kafkaConnectAcl.ReadTopics, kafkaConnectAcl.Principal, sarama.AclOperationRead)
	for j, resource := range resources {
		resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acls[j]}
		i = i + 1
	}

	resources, acls = createTopicAcls(kafkaConnectAcl.WriteTopics, kafkaConnectAcl.Principal, sarama.AclOperationWrite)
	for j, resource := range resources {
		resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acls[j]}
		i = i + 1
	}

	managementTopics := []string{kafkaConnectAcl.OffsetTopic, kafkaConnectAcl.ConfigsTopic, kafkaConnectAcl.StatusTopic}

	operations := []sarama.AclOperation{sarama.AclOperationWrite, sarama.AclOperationRead}

	for _, operation := range operations {
		resources, acls = createTopicAcls(managementTopics, kafkaConnectAcl.Principal, operation)
		for j, resource := range resources {
			resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acls[j]}
			i = i + 1
		}
	}

	if kafkaConnectAcl.EnableTopicCreate {
		resource := sarama.Resource{
			ResourceName:        "kafka-cluster",
			ResourceType:        sarama.AclResourceCluster,
			ResourcePatternType: sarama.AclPatternLiteral,
		}

		acl := sarama.Acl{
			Principal:      kafkaConnectAcl.Principal,
			Host:           "*",
			Operation:      sarama.AclOperationCreate,
			PermissionType: sarama.AclPermissionAllow,
		}
		resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acl}
		i = i + 1
	}

	resource := sarama.Resource{
		ResourceName:        kafkaConnectAcl.Group,
		ResourceType:        sarama.AclResourceGroup,
		ResourcePatternType: sarama.AclPatternLiteral,
	}

	acl := sarama.Acl{
		Principal:      kafkaConnectAcl.Principal,
		Host:           "*",
		Operation:      sarama.AclOperationRead,
		PermissionType: sarama.AclPermissionAllow,
	}
	resourceInfos[i] = AclResourceInfo{Resource: resource, Acl: acl}
	i = i + 1

	return AclResources{Resources: resourceInfos}, nil
}

func GetNumberOfResourcesForKafkaConnect(kafkaConnecAcl KafkaConnectAcl) int {
	resourcesCount := len(kafkaConnecAcl.ReadTopics) + len(kafkaConnecAcl.WriteTopics) + 6 + 1
	if kafkaConnecAcl.EnableTopicCreate {
		resourcesCount = resourcesCount + 1
	}

	return resourcesCount
}

func (b KafkaAclsBuilder) ConsumerAclsParser(d *schema.ResourceData,
	aclInterface interface{},
	aclEntity sarama.ResourceAcls) error {

	consumerAcl := aclInterface.(ConsumerAcl)

	for _, acl := range aclEntity.Acls {
		if acl.Principal == consumerAcl.Principal {
			d.Set("principal", acl.Principal)
			if b.Client.IsAGroupAcl(aclEntity) {
				d.Set("group", aclEntity.ResourceName)
			}
			if b.Client.IsATopicAcl(aclEntity) {
				d.Set("project", aclEntity.ResourceName)
			}
			d.Set("metadata", consumerAcl.Metadata)
		}
	}

	return nil
}

func (b KafkaAclsBuilder) KafkaStreamsAclsParser(d *schema.ResourceData,
	aclInterface interface{},
	aclEntity sarama.ResourceAcls) error {

	kStreamAcl := aclInterface.(KafkaStreamsAcl)
	readTopics := make([]string, 0)
	writeTopics := make([]string, 0)

	for _, acl := range aclEntity.Acls {

		if acl.Principal == kStreamAcl.Principal {
			d.Set("principal", acl.Principal)
			if b.Client.IsAGroupAcl(aclEntity) {
				d.Set("group", aclEntity.ResourceName)
			}
			if b.Client.IsATopicAcl(aclEntity) {
				if aclEntity.ResourcePatternType == sarama.AclPatternPrefixed {
					d.Set("project", aclEntity.ResourceName)
				} else {
					if acl.Operation == sarama.AclOperationRead {
						readTopics = append(readTopics, aclEntity.ResourceName)
					} else {
						writeTopics = append(writeTopics, aclEntity.ResourceName)
					}

				}
			}
			d.Set("read_topics", readTopics)
			d.Set("write_topics", writeTopics)
			d.Set("metadata", kStreamAcl.Metadata)
		}
	}

	return nil
}

func (b KafkaAclsBuilder) KafkaConnectAclsParser(d *schema.ResourceData,
	aclInterface interface{},
	aclEntity sarama.ResourceAcls) error {

	kafkaConnectAcl := aclInterface.(KafkaConnectAcl)
	readTopics := make([]string, 0)
	writeTopics := make([]string, 0)

	controlTopics := make(map[string]bool)
	controlTopics[kafkaConnectAcl.StatusTopic] = true
	controlTopics[kafkaConnectAcl.OffsetTopic] = true
	controlTopics[kafkaConnectAcl.ConfigsTopic] = true

	for _, acl := range aclEntity.Acls {

		if acl.Principal == kafkaConnectAcl.Principal {
			d.Set("principal", acl.Principal)
			if b.Client.IsAGroupAcl(aclEntity) {
				d.Set("group", aclEntity.ResourceName)
			}

			if b.Client.IsATopicAcl(aclEntity) {

				if controlTopics[aclEntity.ResourceName] {
					topicNameKey := ""
					if aclEntity.ResourceName == kafkaConnectAcl.OffsetTopic {
						topicNameKey = "offset_topic"
					} else if aclEntity.ResourceName == kafkaConnectAcl.StatusTopic {
						topicNameKey = "status_topic"
					} else {
						topicNameKey = "configs_topic"
					}
					d.Set(topicNameKey, aclEntity.ResourceName)
				} else {
					if acl.Operation == sarama.AclOperationRead {
						readTopics = append(readTopics, aclEntity.ResourceName)
					} else {
						writeTopics = append(writeTopics, aclEntity.ResourceName)
					}
				}
			}
			if b.Client.IsAClusterAcl(aclEntity) {
				d.Set("enable_topic_create", true)
			}
		}
		d.Set("read_topics", readTopics)
		d.Set("write_topics", writeTopics)
		d.Set("metadata", kafkaConnectAcl.Metadata)
	}

	return nil
}

func (b KafkaAclsBuilder) ConsumerAclShouldContinue(entity sarama.ResourceAcls, aclInterface interface{}) bool {
	consumerAcl := aclInterface.(ConsumerAcl)
	return entity.ResourceName != consumerAcl.Project
}

func (b KafkaAclsBuilder) KafkaStreamsAclShouldContinue(entity sarama.ResourceAcls, aclInterface interface{}) bool {
	acl := aclInterface.(KafkaStreamsAcl)
	return entity.ResourceName != acl.Project
}

func (b KafkaAclsBuilder) KafkaConnectAclShouldContinue(entity sarama.ResourceAcls, aclInterface interface{}) bool {
	aclObject := aclInterface.(KafkaConnectAcl)
	acl := entity.Acls[0]
	return acl.Principal != aclObject.Principal
}
