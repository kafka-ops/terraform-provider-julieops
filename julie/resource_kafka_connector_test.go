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

func TestAccKafkaConnectCreate(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDocker(ctx, julieTest.ContainersSetupConfig{
		EnableSchemaRegistry: true, EnableKafkaConnect: true,
	}, t)
	defer close(ctx)

	connectorName := "foo"
	resource.Test(t, resource.TestCase{
		ProviderFactories: overrideProviderFactory(),
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaConnectorDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(setup.AkContainer.URI, setup.KcContainer.URI, fmt.Sprintf(testResourceConnector, connectorName)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaConnectorExist("julieops_kafka_connector.test"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

const testResourceConnector = `
resource "julieops_kafka_connector" "test" {
  name = "%s"
  config = {
    "connector.class" =                 "io.confluent.kafka.connect.datagen.DatagenConnector"
    "kafka.topic" =                     "pageviews"
    "quickstart" =                      "pageviews"
    "key.converter" =                   "org.apache.kafka.connect.storage.StringConverter"
    "value.converter" =                 "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable" =  "false"
    "max.interval" =                    100
    "iterations" =                      10000000
    "tasks.max" =                       "1"
  }
}
`

func testAccKafkaConnectorDelete(s *terraform.State) error {
	c := testProvider.Meta().(*client.KafkaCluster)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "julieops_kafka_connector" {
			continue
		}
		name := rs.Primary.Attributes["name"]
		c.KafkaConnectClient.DeleteConnector(name)
	}
	return nil
}

func testAccKafkaConnectorExist(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		resource, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("connector not found: %s", name)
		}

		name := resource.Primary.Attributes["name"]

		if name != "foo" {
			return fmt.Errorf("connector found with unexpected name %s", name)
		}

		return nil
	}
}
