package julie

import (
	"context"
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
			"kafkaTopics": &schema.Schema{
				Type: schema.TypeMap,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": &schema.Schema{
							Type: schema.TypeString,
							Computed: true,
						},
						"config": &schema.Schema{
							Type: schema.TypeMap,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{},
							},
						},
					},
				},
			},
		},
	}
}

func dataSourceKafkaTopicsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	var diags diag.Diagnostics
	cluster := client.NewKafkaCluster()

	topics, err := cluster.ListTopics(ctx)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("kafkaTopics", topics)
	d.SetId("1234")

	return diags
}