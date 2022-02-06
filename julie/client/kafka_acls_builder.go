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
