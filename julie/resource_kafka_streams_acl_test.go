package julie

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"terraform-provider-julieops/julie/client"
	"testing"
)

func TestAccKafkaStreamsAclCreate(t *testing.T) {
	project := "foo"
	resource.Test(t, resource.TestCase{
		Providers: testAccProviders,
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaStreamsAclDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(bootstrapServersFromEnv(), fmt.Sprintf(testKafkaStreamsResourceAcl_noConfig, project)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaStreamsAclExist("julieops_kafka_streams_acl.streams", "User:streams"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

const testKafkaStreamsResourceAcl_noConfig = `
resource "julieops_kafka_streams_acl" "streams" {
  project = "%s"
  principal = "User:streams"
  read_topics = [ "foo" ]
  write_topics = [ "bar" ]
  metadata = {
    "foo" = "bar"
  }
}
`

func testAccKafkaStreamsAclDelete(s *terraform.State) error {
	c := testProvider.Meta().(*client.KafkaCluster)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "julieops_kafka_streams_acl" {
			continue
		}
		project := rs.Primary.Attributes["project"]
		principal := rs.Primary.Attributes["principal"]

		//TODO: To be accurate should retrieve the arrays read_topics and write topics, so the acls
		// are not leave in the cluster.... need to find out how...
		acl := client.NewKafkaStreamsAcl(project, principal, []string{}, []string{}, map[string]string{})
		c.DeleteKafkaStreamsAcl(*acl)
	}
	return nil
}

func testAccKafkaStreamsAclExist(resourceName string, principalValue string) resource.TestCheckFunc {
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
