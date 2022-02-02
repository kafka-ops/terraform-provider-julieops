package julie

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-julieops/julie/client"
)

func interfaceAsTopic(d *schema.ResourceData, _interface interface{}) client.Topic {

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
