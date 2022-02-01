package julie

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"terraform-provider-julieops/julie/client"
	"testing"
)

func TestAccKafkaTopicRead(t *testing.T) {
	topicName := "foo"
	resource.Test(t, resource.TestCase{
		Providers: testAccProviders,
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaTopicDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(bootstrapServersFromEnv(), fmt.Sprintf(testResourceTopic_noConfig, topicName)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaTopicExist("julieops_kafka_topic.test"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func TestAccKafkaTopicCreateWithConfig(t *testing.T) {
	topicName := "foo.config"
	resource.Test(t, resource.TestCase{
		Providers: testAccProviders,
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaTopicDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(bootstrapServersFromEnv(), fmt.Sprintf(testResourceTopic_simpleConfig, topicName)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaTopicExist("julieops_kafka_topic.test_config"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

const testResourceTopic_noConfig = `
resource "julieops_kafka_topic" "test" {
  name               = "%s"
  replication_factor = 1
  partitions         = 1
}
`

const testResourceTopic_simpleConfig = `
resource "julieops_kafka_topic" "test_config" {
  name               = "%s"
  replication_factor = 1
  partitions         = 1
  config = {
    "retention.ms": "604800000"
  }
}
`

func cfg(bs string, extraCfg string) string {
	var str = "provider \"julieops\" { \n \t bootstrap_servers = \"%s\" \n } \n %s \n"
	return fmt.Sprintf(str, bs, extraCfg)
}

func testAccKafkaTopicDelete(s *terraform.State) error {
	c := testProvider.Meta().(*client.KafkaCluster)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "julieops_kafka_topic" {
			continue
		}
		topicName := rs.Primary.Attributes["name"]
		err := c.DeleteTopic(ctx, topicName)
		if err != nil {
			return err
		}
	}
	return nil
}

func testAccKafkaTopicExist(topicName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		resource, ok := s.RootModule().Resources[topicName]
		if !ok {
			return fmt.Errorf("topic not found: %s", topicName)
		}

		partitions := resource.Primary.Attributes["partitions"]
		replicationFactor := resource.Primary.Attributes["replication_factor"]
		config := resource.Primary.Attributes["config.retention.ms"]

		if partitions != "1" {
			return fmt.Errorf("topic %s with unexpected partitions number %s", topicName, partitions)
		}

		if replicationFactor != "1" {
			return fmt.Errorf("topic %s with unexpected replicationFactor number %s", topicName, replicationFactor)
		}

		if config != "" && config != "604800000" {
			return fmt.Errorf("topic %s with unexpected config retentino.ms %s", topicName, config)
		}

		return nil
	}
}
