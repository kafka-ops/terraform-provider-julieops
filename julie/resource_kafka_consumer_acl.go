package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func resourceKafkaConsumerAcl() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceKafkaConsumerCreate,
		ReadContext:   resourceKafkaConsumerRead,
		UpdateContext: resourceKafkaConsumerUpdate,
		DeleteContext: resourceKafkaConsumerDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"project": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: "The project prefix used to build the resource ACLs",
			},
			"principal": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: "The user principal for this acl definition.",
			},
			"group": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "*",
				Description: "The consumer group name.",
			},
			"metadata": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "Map of optional values describing metadata information for this consumer",
				Elem:        schema.TypeString,
			},
		},
	}
}

func resourceKafkaConsumerCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	kafkaClient := m.(*client.KafkaCluster)
	builder := client.KafkaAclsBuilder{
		Client: kafkaClient,
	}

	aclInterface, err := funcCreateAcl(kafkaClient, builder, d, resourceAsConsumerAcl, builder.ConsumerAclsBuilder)
	if err != nil {
		return diag.FromErr(err)
	}
	acl := aclInterface.(client.ConsumerAcl)
	d.SetId(acl.Id)
	return nil
}

func resourceKafkaConsumerRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	kafkaClient := m.(*client.KafkaCluster)
	log.Printf("[DEBUG] consumerAclRead: consumerAcl=%s", d.Id())

	builder := client.KafkaAclsBuilder{
		Client: kafkaClient,
	}
	consumerAcl := resourceAsConsumerAcl(d).(client.ConsumerAcl)

	foundAcls, err := kafkaClient.ListAcls(consumerAcl.Principal)
	if err != nil {
		return diag.FromErr(err)
	}

	funcSelectAclsFor(d, foundAcls, kafkaClient, consumerAcl, builder.ConsumerAclShouldContinue, builder.ConsumerAclsParser)

	return nil
}

func resourceKafkaConsumerUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.KafkaCluster)
	acl := resourceAsConsumerAcl(d).(client.ConsumerAcl)

	_, err := c.CreateConsumerAcl(acl)

	if err != nil {
		return diag.FromErr(err)
	}

	return resourceKafkaTopicRead(ctx, d, m)
}

func resourceKafkaConsumerDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	c := m.(*client.KafkaCluster)
	acl := resourceAsConsumerAcl(d).(client.ConsumerAcl)

	log.Printf("[DEBUG] Deleting consumer ACL(s) for %s", acl)

	err := c.DeleteConsumerAcl(acl)

	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
