package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"terraform-provider-julieops/julie/client"
)

// Provider -
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"bootstrap_servers": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "A list of kafka brokers",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"julieops_kafka_topic": resourceKafkaTopic(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"julieops_kafka_topic": dataSourceKafkaTopics(),
		},
		ConfigureContextFunc: providerConfig,
	}
}

func providerConfig(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	bootstrapServers := d.Get("bootstrap_servers").(string)

	var diags diag.Diagnostics

	if bootstrapServers != "" {
		cluster := client.NewKafkaCluster(bootstrapServers)
		return cluster, diags
	}
	return nil, diags
}
