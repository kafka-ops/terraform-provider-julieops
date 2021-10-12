package julie

import (
	"context"
	"log"
	"strconv"
	"terraform-provider-julieops/julie/client"
	"time"

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

func dataSourceKafkaTopicsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	name := d.Get("name").(string)

	var diags diag.Diagnostics

	cluster := m.(*client.KafkaCluster)

	topics, err := cluster.ListTopics(ctx, &name, false)
	if err != nil {
		return diag.FromErr(err)
	}

	for _, topic := range topics {
		log.Printf("DEBUG topicsRead: topic=%s", topic.Name)
		d.Set("name", topic.Name)
		d.Set("partitions", topic.NumPartitions)
		d.Set("replication_factor", topic.ReplicationFactor)
		d.Set("config", topic.Config)
	}
	d.SetId(strconv.FormatInt(time.Now().Unix(), 10))

	return diags
}
