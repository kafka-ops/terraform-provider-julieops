package client

import (
	"testing"
)
import "github.com/stretchr/testify/assert"

func TestKafkaConnectClientDefault(t *testing.T) {

	client := NewKafkaConnectClient("http://localhost:18083")
	response, err := client.GetClusterInfo()
	if err != nil {
		t.Errorf("Something happen while getting cluster info: %s", err)
	}

	assert.NotEmpty(t, response.KafkaClusterId, "ClusterID should not not empty")
	assert.NotEmpty(t, response.Commit, "Commit should not not empty")
	assert.NotEmpty(t, response.Version, "Version should not not empty")
}

func TestKafkaConnectCluster_GetConnectors(t *testing.T) {
	client := NewKafkaConnectClient("http://localhost:18083")
	response, err := client.GetConnectors()
	if err != nil {
		t.Errorf("Something happen while getting the connectors info: %s", err)
	}

	assert.Empty(t, response.Connectors, "Connectors should be empty if no connector has been created")
}

func TestKafkaConnectCluster_AddConnector(t *testing.T) {
	client := NewKafkaConnectClient("http://localhost:18083")

	var connectorConfig = map[string]interface{}{
		"connector.class":                "io.confluent.kafka.connect.datagen.DatagenConnector",
		"kafka.topic":                    "pageviews",
		"quickstart":                     "pageviews",
		"key.converter":                  "org.apache.kafka.connect.storage.StringConverter",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
		"max.interval":                   100,
		"iterations":                     10000000,
		"tasks.max":                      "1",
	}
	var connector = ConnectorCreateRequest{
		Name:   "foo",
		Config: connectorConfig,
	}
	defer client.DeleteConnector("foo")
	response, err := client.AddConnector(connector)

	if err != nil {
		t.Errorf("Something happen while trying to create a connector : %s", err)
	}
	assert.NotEmpty(t, response.Name, "Name should be not empty")
}
