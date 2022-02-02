package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"reflect"
	"terraform-provider-julieops/julie/client"
)

func resourceKafkaTopic() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceKafkaTopicCreate,
		ReadContext:   resourceKafkaTopicRead,
		UpdateContext: resourceKafkaTopicUpdate,
		DeleteContext: resourceKafkaTopicDelete,
		CustomizeDiff: customDiff,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: "The name of the topic.",
			},
			"partitions": {
				Type:        schema.TypeInt,
				Required:    true,
				Description: "Number of partitions.",
			},
			"replication_factor": {
				Type:        schema.TypeInt,
				Required:    true,
				ForceNew:    false,
				Description: "Number of replicas.",
			},
			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "A map of string k/v attributes.",
				Elem:        schema.TypeString,
			},
		},
	}
}

func resourceKafkaTopicCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.KafkaCluster)
	t := interfaceAsTopic(d)

	topic, err := c.CreateTopic(ctx, t.Name, t.NumPartitions, t.ReplicationFactor, t.Config)

	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(topic.Name)
	return nil
}

func resourceKafkaTopicRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	name := d.Id()
	c := m.(*client.KafkaCluster)

	topic, err := c.ListTopics(ctx, name)

	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("DEBUG resourceKafkaTopicRead: name= %s, topic= %s, len= %d", name, topic[0].Name, len(topic))
	if len(topic) > 0 {
		d.Set("name", topic[0].Name)
		d.Set("partitions", topic[0].NumPartitions)
		d.Set("replication_factor", topic[0].ReplicationFactor)
		d.Set("config", topic[0].Config)
		d.SetId(topic[0].Name)
	}

	return nil
}

func resourceKafkaTopicUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	c := m.(*client.KafkaCluster)

	t := interfaceAsTopic(d)
	log.Printf("DEBUG resourceKafkaTopicUpdate: name=%s config.keys=%s", t.Name, reflect.ValueOf(t.Config).MapKeys())
	err := c.UpdateTopic(ctx, t.Name, t.Config)

	if err != nil {
		return diag.FromErr(err)
	}
	return resourceKafkaTopicRead(ctx, d, m)
}

func resourceKafkaTopicDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	c := m.(*client.KafkaCluster)
	name := d.Id()
	err := c.DeleteTopic(ctx, name)

	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func customDiff(ctx context.Context, diff *schema.ResourceDiff, m interface{}) error {
	log.Printf("Checking the difference")

	//c := m.(*client.KafkaCluster)

	if diff.HasChange("partitions") {
		oldPartitions, newPartitions := diff.GetChange("partitions")
		oldInt := oldPartitions.(int)
		newInt := newPartitions.(int)
		log.Printf("[INFO] Partitions have changed, old = %d, new = %d", oldInt, newInt)

		if newInt < oldInt {
			log.Printf("Partition can not be decresed")
			if err := diff.ForceNew("partitions"); err != nil {
				return err
			}
		}
	}

	if diff.HasChange("config") {
		//oldConfig, newConfig := diff.GetChange("config")
	}

	return nil
}
