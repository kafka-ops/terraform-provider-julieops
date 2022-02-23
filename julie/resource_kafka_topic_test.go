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

func TestAccKafkaTopicCreateWithoutConfig(t *testing.T) {

	ctx := context.Background()
	setup, close := julieTest.SetupDocker(ctx, julieTest.ContainersSetupConfig{}, t)
	defer close(ctx)

	topicName := "foo"
	resource.Test(t, resource.TestCase{
		ProviderFactories: overrideProviderFactory(),
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaTopicDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(setup.AkContainer.URI, kafkaConnectServerFromEnv(), fmt.Sprintf(testResourceTopic_noConfig, topicName)),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaTopicExist("julieops_kafka_topic.test", ""),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func TestAccKafkaTopicCreateWithConfig(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDocker(ctx, julieTest.ContainersSetupConfig{}, t)
	defer close(ctx)

	topicName := "foo.config"
	resource.Test(t, resource.TestCase{
		ProviderFactories: overrideProviderFactory(),
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaTopicDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(setup.AkContainer.URI, kafkaConnectServerFromEnv(), fmt.Sprintf(testResourceTopic_simpleConfig, topicName, "42")),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaTopicExist("julieops_kafka_topic.test_config", "42"),
				),
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func TestAccKafkaTopicConfigUpdate(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDocker(ctx, julieTest.ContainersSetupConfig{}, t)
	defer close(ctx)

	topicName := "foo.config.update"
	resource.Test(t, resource.TestCase{
		ProviderFactories: overrideProviderFactory(),
		PreCheck: func() {
			testAccPreCheck(t)
		},
		CheckDestroy: testAccKafkaTopicDelete,
		Steps: []resource.TestStep{
			{
				Config: cfg(setup.AkContainer.URI, kafkaConnectServerFromEnv(), fmt.Sprintf(testResourceTopic_simpleConfig, topicName, "42")),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaTopicExist("julieops_kafka_topic.test_config", "42"),
				),
				ExpectNonEmptyPlan: false,
			},
			{
				Config: cfg(bootstrapServersFromEnv(), kafkaConnectServerFromEnv(), fmt.Sprintf(testResourceTopic_simpleConfig, topicName, "24")),
				Check: resource.ComposeTestCheckFunc(
					testAccKafkaTopicExist("julieops_kafka_topic.test_config", "24"),
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
    "retention.ms": "%s"
  }
}
`

/*func cfg(bs string, extraCfg string) string {
	var saslConfig = " \t sasl_username =  \"kafka\" \n \t sasl_password = \"kafka\" \n \t sasl_mechanism = \"plain\"  \n "
	var str = "provider \"julieops\" { \n \t bootstrap_servers = \"%s\" \n %s } \n %s \n"
	//log.Printf(str, bs, saslConfig, extraCfg)
	return fmt.Sprintf(str, bs, saslConfig, extraCfg)
}*/

func cfg(bs string, connect string, extraCfg string) string {
	var saslConfig = " \t sasl_username =  \"kafka\" \n \t sasl_password = \"kafka\" \n \t sasl_mechanism = \"plain\"  \n "
	var connectConfig = fmt.Sprintf("\t kafka_connects = \"%s\" \n", connect)
	var str = "provider \"julieops\" { \n \t bootstrap_servers = \"%s\" \n %s %s } \n %s \n"
	//fmt.Printf("[DEBUG] cfg: %s", fmt.Sprintf(str, bs, connectConfig, saslConfig, extraCfg))
	return fmt.Sprintf(str, bs, connectConfig, saslConfig, extraCfg)
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
		c.DeleteTopic(ctx, topicName)
	}
	return nil
}

func testAccKafkaTopicExist(topicName string, retentionMsValue string) resource.TestCheckFunc {
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

		if config != "" && config != retentionMsValue {
			return fmt.Errorf("topic %s with unexpected config retentino.ms %s", topicName, config)
		}

		return nil
	}
}
