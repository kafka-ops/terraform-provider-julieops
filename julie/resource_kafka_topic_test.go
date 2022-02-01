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

const testResourceTopic_noConfig = `
resource "julieops_kafka_topic" "test" {
  name               = "%s"
  replication_factor = 1
  partitions         = 1
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
		/*for i, v := range s.RootModule().Resources {
			fmt.Printf("%s ** %s", i, v)
		}
		fmt.Printf("F: %s", s.RootModule().String())*/
		_, ok := s.RootModule().Resources[topicName]
		if !ok {
			return fmt.Errorf("topic not found: %s", topicName)
		}
		return nil
	}
}
