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
		UpdateContext: resourceKafkaStreamsUpdate,
		DeleteContext: resourceKafkaStreamsDelete,
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
				Description: "The user principal for this acl definition",
			},
			"read_topics": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "The collection of source topics for the Kafka Streams application",
			},
			"write_topics": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Description: "The collection of write topics for the Kafka Streams application",
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
	/*c := m.(*client.KafkaCluster)
	log.Printf("[DEBUG] consumerAclRead: kStreamAcl=%s", d.Id())

	kStreamAcl := resourceAsKafkaStreamsAcl(d).(client.KafkaStreamsAcl)

	foundAcls, err := c.ListAcls(kStreamAcl.Principal)
	if err != nil {
		return diag.FromErr(err)
	}

	for _, aclEntity := range foundAcls {
		if aclEntity.ResourceName != kStreamAcl.Project {
			continue
		}

		if len(aclEntity.Acls) < 1 {
			break
		}
		log.Printf("[INFO] ACL(s) found resource %s, acls.Count = %d", aclEntity.ResourceName, len(aclEntity.Acls))

		readTopics := make([]string, 0)
		writeTopics := make([]string, 0)

		for _, acl := range aclEntity.Acls {

			if acl.Principal == kStreamAcl.Principal {
				d.Set("principal", acl.Principal)
				if c.IsAGroupAcl(aclEntity) {
					d.Set("group", aclEntity.ResourceName)
				}
				if c.IsATopicAcl(aclEntity) {
					if aclEntity.ResourcePatternType == sarama.AclPatternPrefixed {
						d.Set("project", aclEntity.ResourceName)
					} else {
						if acl.Operation == sarama.AclOperationRead {
							readTopics = append(readTopics, aclEntity.ResourceName)
						} else {
							writeTopics = append(writeTopics, aclEntity.ResourceName)
						}

					}
				}
				d.Set("read_topics", readTopics)
				d.Set("write_topics", writeTopics)
				d.Set("metadata", kStreamAcl.Metadata)
			}
		}
	}*/

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

	funcSelectAclsFor(d, foundAcls, kafkaClient, kStreamAcl, builder.KafkaStreamsAclShouldContinue, builder.KafkaStreamsAclsParser)

	return nil

	return nil
}

func resourceKafkaStreamsUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.KafkaCluster)
	acl := resourceAsKafkaStreamsAcl(d).(client.KafkaStreamsAcl)

	_, err := c.CreateKafkaStreamsAcl(acl)

	if err != nil {
		return diag.FromErr(err)
	}

	return resourceKafkaStreamsRead(ctx, d, m)
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
