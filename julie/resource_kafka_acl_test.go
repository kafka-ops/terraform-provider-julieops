package julie

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"terraform-provider-julieops/julie/client"
	"testing"
)

func TestAccKafkaAclCreate(t *testing.T) {
	project := "foo"
	resource.Test(t, resource.TestCase{
		Providers: testAccProviders,
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaAclDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(bootstrapServersFromEnv(), fmt.Sprintf(testResourceAcl_noConfig, project)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaAclExist("julieops_kafka_acl_consumer.consumer", "User:bar"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

const testResourceAcl_noConfig = `
resource "julieops_kafka_acl_consumer" "consumer" {
  project = "%s"
  principal = "User:bar"
  group = "*"
  metadata = {
    "foo" = "bar"
  }
}
`

func testAccKafkaAclDelete(s *terraform.State) error {
	c := testProvider.Meta().(*client.KafkaCluster)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "julieops_kafka_acl_consumer" {
			continue
		}
		project := rs.Primary.Attributes["project"]
		principal := rs.Primary.Attributes["principal"]
		group := rs.Primary.Attributes["group"]

		consumerAcl := client.NewConsumerAcl(project, principal, group, map[string]string{})
		c.DeleteConsumerAcl(*consumerAcl)
	}
	return nil
}

func testAccKafkaAclExist(resourceName string, principalValue string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		resource, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("ACL(s) not found: %s", resourceName)
		}

		principal := resource.Primary.Attributes["principal"]

		if principal != principalValue {
			return fmt.Errorf("acl(s) %s with unexpected principal %s", resourceName, principal)
		}

		return nil
	}
}