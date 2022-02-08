package julie

import (
	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func funcCreateAcl(c *client.KafkaCluster, builder client.KafkaAclsBuilder,
	d *schema.ResourceData, fnConvert client.Convert, fnBuilder client.AclBuilder) (interface{}, error) {

	aclInterface, acls, err := builder.BuildAcls(d, fnConvert, fnBuilder)

	if err != nil {
		return nil, err
	}

	err = c.ApplyAcls(acls)

	if err != nil {
		return nil, err
	}

	return aclInterface, nil
}

type fnShouldContinue func(entity sarama.ResourceAcls, aclInterface interface{}) bool
type fnAclParser func(d *schema.ResourceData, aclInterface interface{}, aclEntity sarama.ResourceAcls) error

func funcSelectAclsFor(d *schema.ResourceData, foundAcls []sarama.ResourceAcls, aclInterface interface{},
	shouldContinue fnShouldContinue, parser fnAclParser) {
	for _, aclEntity := range foundAcls {
		if shouldContinue(aclEntity, aclInterface) {
			continue
		}
		if len(aclEntity.Acls) < 1 {
			break
		}
		log.Printf("[INFO] ACL(s) found resource %s, acls.Count = %d", aclEntity.ResourceName, len(aclEntity.Acls))
		parser(d, aclInterface, aclEntity)
	}
}
