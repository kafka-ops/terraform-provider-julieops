package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func resourceKafkaStreamsAcl() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceKafkaStreamsCreate,
		ReadContext:   resourceKafkaStreamsRead,
		DeleteContext: resourceKafkaStreamsDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"project": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The project prefix used to build the resource ACLs",
			},
			"principal": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The user principal for this acl definition",
			},
			"read_topics": {
				Type:     schema.TypeList,
				ForceNew: true,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "The collection of source topics for the Kafka Streams application",
			},
			"write_topics": {
				Type:     schema.TypeList,
				ForceNew: true,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "The collection of write topics for the Kafka Streams application",
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

func resourceKafkaStreamsCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	kafkaClient := m.(*client.KafkaCluster)
	builder := client.KafkaAclsBuilder{
		Client: kafkaClient,
	}

	aclInterface, err := funcCreateAcl(kafkaClient, builder, d, resourceAsKafkaStreamsAcl, builder.KafkaStreamsAclsBuilder)
	if err != nil {
		return diag.FromErr(err)
	}
	acl := aclInterface.(client.KafkaStreamsAcl)
	d.SetId(acl.Id)
	return nil
}

func resourceKafkaStreamsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	kafkaClient := m.(*client.KafkaCluster)
	log.Printf("[DEBUG] KafkaStreamsAclRead: kStreamAcl=%s", d.Id())

	builder := client.KafkaAclsBuilder{
		Client: kafkaClient,
	}
	kStreamAcl := resourceAsKafkaStreamsAcl(d).(client.KafkaStreamsAcl)

	foundAcls, err := kafkaClient.ListAcls(kStreamAcl.Principal)
	if err != nil {
		return diag.FromErr(err)
	}

	funcSelectAclsFor(d, foundAcls, kStreamAcl, builder.KafkaStreamsAclShouldContinue, builder.KafkaStreamsAclsParser)

	return nil

	return nil
}

func resourceKafkaStreamsDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	c := m.(*client.KafkaCluster)
	acl := resourceAsKafkaStreamsAcl(d).(client.KafkaStreamsAcl)

	log.Printf("[DEBUG] Deleting Kafka Streams ACL(s) for %s", acl)

	err := c.DeleteKafkaStreamsAcl(acl)

	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
