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

func resourceAsConsumerAcl(d *schema.ResourceData) interface{} {

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

func resourceAsKafkaStreamsAcl(d *schema.ResourceData) interface{} {
	project := d.Get("project").(string)
	principal := d.Get("principal").(string)
	readTopics := d.Get("read_topics").([]interface{})
	writeTopics := d.Get("write_topics").([]interface{})

	metadata := d.Get("metadata").(map[string]interface{})

	metaMap := make(map[string]string)
	for k, v := range metadata {
		switch v := v.(type) {
		case string:
			log.Printf("[DEBUG] resourceAsKafkaStreamsAcl: config.key = %s, config.value = %s", k, v)
			metaMap[k] = v
		}
	}

	readTopicsArray := interfaceArrayAsSlice(readTopics)
	writeTopicsArray := interfaceArrayAsSlice(writeTopics)

	return *client.NewKafkaStreamsAcl(project, principal, readTopicsArray, writeTopicsArray, metaMap)
}

func resourceAsKafkaConnectAcl(d *schema.ResourceData) interface{} {
	principal := d.Get("principal").(string)
	readTopics := d.Get("read_topics").([]interface{})
	writeTopics := d.Get("write_topics").([]interface{})
	group := d.Get("group").(string)
	statusTopic := d.Get("status_topic").(string)
	configsTopic := d.Get("configs_topic").(string)
	offsetTopic := d.Get("offset_topic").(string)
	topicCreate := d.Get("enable_topic_create").(bool)

	metadata := d.Get("metadata").(map[string]interface{})

	metaMap := make(map[string]string)
	for k, v := range metadata {
		switch v := v.(type) {
		case string:
			log.Printf("[DEBUG] resourceAsKafkaConnectAcl: config.key = %s, config.value = %s", k, v)
			metaMap[k] = v
		}
	}

	readTopicsArray := interfaceArrayAsSlice(readTopics)
	writeTopicsArray := interfaceArrayAsSlice(writeTopics)

	return *client.NewKafkaConnectAcl(principal, group, readTopicsArray, writeTopicsArray, statusTopic, configsTopic, offsetTopic, topicCreate, metaMap)
}

func interfaceArrayAsSlice(topics []interface{}) []string {
	topicsArray := make([]string, len(topics))
	for i, topic := range topics {
		switch v := topic.(type) {
		case string:
			log.Printf("[DEBUG] resourceAsKafkaStreamsAcl: topics.key = %d, topics.value = %s", i, topic)
			topicsArray[i] = v
		}
	}
	return topicsArray
}
