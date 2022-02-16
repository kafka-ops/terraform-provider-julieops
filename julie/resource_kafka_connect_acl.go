package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func resourceKafkaConnectAcl() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceKafkaConnectCreate,
		ReadContext:   resourceKafkaConnectRead,
		DeleteContext: resourceKafkaConnectDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"principal": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The user principal for this acl definition",
			},
			"group": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "connect-cluster",
				ForceNew:    true,
				Description: "The group used for this Kafka Connect deployment",
			},
			"read_topics": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "The collection of source topics for the Kafka Streams application",
			},
			"write_topics": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "The collection of write topics for the Kafka Streams application",
			},
			"status_topic": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "connect-status",
				ForceNew:    true,
				Description: "The status topic used for the connect cluster",
			},
			"offset_topic": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "connect-offsets",
				ForceNew:    true,
				Description: "The offset topic used for the connect cluster",
			},
			"configs_topic": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "connect-configs",
				ForceNew:    true,
				Description: "The configs topic used for the connect cluster",
			},
			"enable_topic_create": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				ForceNew:    true,
				Description: "True if the connect cluster can create their own topics",
			},
			"metadata": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    true,
				Description: "Map of optional values describing metadata information for this consumer",
				Elem:        schema.TypeString,
			},
		},
	}
}

func resourceKafkaConnectCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	kafkaClient := m.(*client.KafkaCluster)
	builder := client.KafkaAclsBuilder{
		Client: kafkaClient,
	}

	aclInterface, err := funcCreateAcl(kafkaClient, builder, d, resourceAsKafkaConnectAcl, builder.KafkaConnectAclsBuilder)
	if err != nil {
		return diag.FromErr(err)
	}
	acl := aclInterface.(client.KafkaConnectAcl)
	d.SetId(acl.Id)
	return nil
}

func resourceKafkaConnectRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	kafkaClient := m.(*client.KafkaCluster)
	log.Printf("[DEBUG] resourceKafkaConnectRead: KafkaConnectAcl=%s", d.Id())

	builder := client.KafkaAclsBuilder{
		Client: kafkaClient,
	}
	kafkaConnectAcl := resourceAsKafkaConnectAcl(d).(client.KafkaConnectAcl)

	foundAcls, err := kafkaClient.ListAcls(kafkaConnectAcl.Principal)
	if err != nil {
		return diag.FromErr(err)
	}

	funcSelectAclsFor(d, foundAcls, kafkaConnectAcl, builder.KafkaConnectAclShouldContinue, builder.KafkaConnectAclsParser)

	return nil
}

func resourceKafkaConnectDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	c := m.(*client.KafkaCluster)
	builder := client.KafkaAclsBuilder{Client: c}

	acl := resourceAsKafkaConnectAcl(d).(client.KafkaConnectAcl)

	log.Printf("[DEBUG] Deleting Kafka Connect ACL(s) for %s", acl.Id)

	err := c.DeleteKafkaConnectAcl(acl, builder)

	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
