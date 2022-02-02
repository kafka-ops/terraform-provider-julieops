package julie

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func interfaceAsTopic(d *schema.ResourceData) client.Topic {

	name := d.Get("name").(string)
	partitions := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	config := d.Get("config").(map[string]interface{})

	mapConfig := make(map[string]*string)
	for k, v := range config {
		switch v := v.(type) {
		case string:
			log.Printf("interfaceAsTopic: config.key = %s, config.value = %s", k, v)
			mapConfig[k] = &v
		}
	}
	return client.Topic{
		Name:              name,
		ReplicationFactor: replicationFactor,
		NumPartitions:     partitions,
		Config:            mapConfig,
	}
}

func resourceAsConsumerAcl(d *schema.ResourceData) client.ConsumerAcl {

	project := d.Get("project").(string)
	principal := d.Get("principal").(string)
	group := d.Get("group").(string)

	metadata := d.Get("metadata").(map[string]interface{})

	metaMap := make(map[string]string)
	for k, v := range metadata {
		switch v := v.(type) {
		case string:
			log.Printf("[DEBUG] resourceAsConsumerAcl: config.key = %s, config.value = %s", k, v)
			metaMap[k] = v
		}
	}

	return *client.NewConsumerAcl(project, principal, group, metaMap)
}
