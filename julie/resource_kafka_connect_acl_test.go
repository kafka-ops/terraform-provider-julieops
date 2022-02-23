package julie

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"terraform-provider-julieops/julie/client"
	julieTest "terraform-provider-julieops/julie/test"
	"testing"
)

func TestAccKafkaConnectAclCreate(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDocker(ctx, julieTest.ContainersSetupConfig{}, t)
	defer close(ctx)

	principal := "User:connect"
	resource.Test(t, resource.TestCase{
		ProviderFactories: overrideProviderFactory(),
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaConnectAclDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(setup.AkContainer.URI, kafkaConnectServerFromEnv(), fmt.Sprintf(testKafkaConnectResourceAcl_noConfig, principal)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaConnectAclExist("julieops_kafka_connect_acl.connect", "User:connect"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

const testKafkaConnectResourceAcl_noConfig = `
resource "julieops_kafka_connect_acl" "connect" {
  principal = "%s"
  read_topics = [ "foo" ]
  write_topics = [ "bar" ]
  metadata = {
    "foo" = "bar"
  }
}
`

func testAccKafkaConnectAclDelete(s *terraform.State) error {
	c := testProvider.Meta().(*client.KafkaCluster)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "julieops_kafka_connect_acl" {
			continue
		}
		principal := rs.Primary.Attributes["principal"]
		statusTopic := rs.Primary.Attributes["status_topic"]
		configsTopic := rs.Primary.Attributes["configs_topic"]
		offsetTopic := rs.Primary.Attributes["offset_topic"]

		//TODO: To be accurate should retrieve the arrays read_topics and write topics, so the acls
		// are not leave in the cluster.... need to find out how...
		acl := client.NewKafkaConnectAcl(principal, "", []string{}, []string{},
			statusTopic, configsTopic, offsetTopic, false, map[string]string{})
		c.DeleteKafkaConnectAcl(*acl, client.KafkaAclsBuilder{Client: c})
	}
	return nil
}

func testAccKafkaConnectAclExist(resourceName string, principalValue string) resource.TestCheckFunc {
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
