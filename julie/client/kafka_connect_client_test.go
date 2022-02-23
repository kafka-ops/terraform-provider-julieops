package client

import (
	"context"
	julieTest "terraform-provider-julieops/julie/test"
	"testing"
)
import "github.com/stretchr/testify/assert"

func TestKafkaConnectClientDefault(t *testing.T) {

	ctx := context.Background()
	setup, close := julieTest.SetupDockerWithPath(ctx, julieTest.ContainersSetupConfig{
		EnableSchemaRegistry: true,
		EnableKafkaConnect:   true,
		RootPath:             julieTest.AbsoluteMountPath("docker/res/", "/../../"),
	}, t)
	defer close(ctx)

	client := NewKafkaConnectClient(setup.KcContainer.URI)
	response, err := client.GetClusterInfo()
	if err != nil {
		t.Errorf("Something happen while getting cluster info: %s", err)
	}

	assert.NotEmpty(t, response.KafkaClusterId, "ClusterID should not not empty")
	assert.NotEmpty(t, response.Commit, "Commit should not not empty")
	assert.NotEmpty(t, response.Version, "Version should not not empty")
}

func TestKafkaConnectCluster_GetConnectors(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDockerWithPath(ctx, julieTest.ContainersSetupConfig{
		EnableSchemaRegistry: true,
		EnableKafkaConnect:   true,
		RootPath:             julieTest.AbsoluteMountPath("docker/res/", "/../../"),
	}, t)
	defer close(ctx)

	client := NewKafkaConnectClient(setup.KcContainer.URI)
	response, err := client.GetConnectors()
	if err != nil {
		t.Errorf("Something happen while getting the connectors info: %s", err)
	}

	assert.Empty(t, response.Connectors, "Connectors should be empty if no connector has been created")
}

func TestKafkaConnectCluster_AddConnector(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDockerWithPath(ctx, julieTest.ContainersSetupConfig{
		EnableSchemaRegistry: true,
		EnableKafkaConnect:   true,
		RootPath:             julieTest.AbsoluteMountPath("docker/res/", "/../../"),
	}, t)
	defer close(ctx)

	client := NewKafkaConnectClient(setup.KcContainer.URI)

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

func TestKafkaConnectCluster_GetConnector(t *testing.T) {
	ctx := context.Background()
	setup, close := julieTest.SetupDockerWithPath(ctx, julieTest.ContainersSetupConfig{
		EnableSchemaRegistry: true,
		EnableKafkaConnect:   true,
		RootPath:             julieTest.AbsoluteMountPath("docker/res/", "/../../"),
	}, t)
	defer close(ctx)

	client := NewKafkaConnectClient(setup.KcContainer.URI)

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

	response, err := client.AddOrUpdateConnector(connector)

	if err != nil {
		t.Errorf("Something happen while trying to create a connector : %s", err)
	}
	assert.NotEmpty(t, response.Name, "Name should be not empty")

	getConnectorResponse, err := client.GetConnector("foo")

	if err != nil {
		t.Errorf("Something happen while trying to get the connector foo : %s", err)
	}

	assert.NotEmpty(t, getConnectorResponse.Name, "Name should be not empty")
}
