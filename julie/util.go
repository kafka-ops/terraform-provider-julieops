package julie

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
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
