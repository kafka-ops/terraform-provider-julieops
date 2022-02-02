package julie

import (
	"context"
	"log"
	"terraform-provider-julieops/julie/client"
	//	"encoding/json"
	//	"fmt"
	//	"net/http"
	//	"strconv"
	//	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceKafkaTopics() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceKafkaTopicsRead,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the topic.",
			},
			"partitions": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Number of partitions.",
			},
			"replication_factor": {
				Type:        schema.TypeInt,
				Computed:    true,
				ForceNew:    false,
				Description: "Number of replicas.",
			},
			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				Computed:    true,
				ForceNew:    false,
				Description: "A map of string k/v attributes.",
				Elem:        schema.TypeString,
			},
		},
	}
}

func dataSourceKafkaTopicsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	name := d.Get("name").(string)

	var diags diag.Diagnostics

	cluster := m.(*client.KafkaCluster)

	topics, err := cluster.ListTopics(ctx, name)
	if err != nil {
		return diag.FromErr(err)
	}

	for _, topic := range topics {
		log.Printf("DEBUG dataSourceKafkaTopicsRead: name=%s, topic=%s, len=%d", name, topic.Name, len(topics))
		if topic.Name == "" {
			continue
		}
		d.Set("name", topic.Name)
		d.Set("partitions", topic.NumPartitions)
		d.Set("replication_factor", topic.ReplicationFactor)
		d.Set("config", topic.Config)
		d.SetId(topic.Name)
	}
	//d.SetId(strconv.FormatInt(time.Now().Unix(), 10))

	return diags
}
