package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func resourceKafkaConnector() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceKafkaConnectorCreate,
		ReadContext:   resourceKafkaConnectorRead,
		DeleteContext: resourceKafkaConnectorDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the connector.",
			},
			"config": {
				Type:        schema.TypeMap,
				Required:    true,
				ForceNew:    true,
				Description: "A map of string k/v attributes.",
				Elem:        schema.TypeString,
			},
		},
	}
}

func resourceKafkaConnectorCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.KafkaCluster)

	connectorData := extractConnectorResource(d)

	var request = client.ConnectorCreateRequest{
		Name:   connectorData.Name,
		Config: connectorData.Config,
	}

	response, err := c.KafkaConnectClient.AddOrUpdateConnector(request)

	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] resource Kafka Connector Create connector with name %s and Tasks %v", connectorData.Name, response.Tasks)

	d.SetId(response.Name)

	return nil
}

func resourceKafkaConnectorRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.KafkaCluster)

	name := d.Id()

	response, err := c.KafkaConnectClient.GetConnector(name)

	if err != nil {
		return diag.FromErr(err)
	}

	delete(response.Config, "name") // remove the name parameter from config as it causes conflicts

	log.Printf("[DEBUG] reading the information about connector %s", name)
	log.Printf("[DEBUG] read config -> %v", response.Config)

	d.Set("name", response.Name)
	d.Set("config", response.Config)
	d.SetId(response.Name)

	return nil
}

func resourceKafkaConnectorDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	c := m.(*client.KafkaCluster)
	name := d.Id()

	err := c.KafkaConnectClient.DeleteConnector(name)
	log.Printf("[DEBUG] deleting connector with name %s", name)

	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
