package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"terraform-provider-julieops/julie/client"
)

func resourceKafkaTopic() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceKafkaTopicCreate,
		ReadContext:   resourceKafkaTopicRead,
		UpdateContext: resourceKafkaTopicUpdate,
		DeleteContext: resourceKafkaTopicDelete,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
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
	var diags diag.Diagnostics

	c := m.(*client.KafkaCluster)
	t := interfaceAsTopic(d, m)

	topic, err := c.CreateTopic(ctx, t.Name, t.NumPartitions, t.ReplicationFactor)

	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(topic.Name)
	return diags
}

func resourceKafkaTopicRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	name := d.Id()
	c := m.(client.KafkaCluster)

	topic, err := c.ListTopics(ctx, &name, false)

	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("name", topic[0].Name)
	d.Set("partitions", topic[0].NumPartitions)
	d.Set("replication_factor", topic[0].ReplicationFactor)

	return diags
}

func resourceKafkaTopicUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	return resourceKafkaTopicRead(ctx, d, m)
}

func resourceKafkaTopicDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics
	return diags
}
