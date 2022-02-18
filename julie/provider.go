package julie

import (
	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"terraform-provider-julieops/julie/client"
)

// Provider -
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"bootstrap_servers": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "A list of kafka brokers",
			},
			"sasl_username": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The Sasl username",
			},
			"sasl_password": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The sasl password",
			},
			"sasl_mechanism": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The sasl mechanism to be used",
			},
			"kafka_connects": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The Kafka Connect cluster url(s)",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"julieops_kafka_topic":        resourceKafkaTopic(),
			"julieops_kafka_consumer_acl": resourceKafkaConsumerAcl(),
			"julieops_kafka_streams_acl":  resourceKafkaStreamsAcl(),
			"julieops_kafka_connect_acl":  resourceKafkaConnectAcl(),
			"julieops_kafka_connector":    resourceKafkaConnector(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"julieops_kafka_topic": dataSourceKafkaTopics(),
		},
		ConfigureContextFunc: providerConfig,
	}
}

func providerConfig(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	bootstrapServers := d.Get("bootstrap_servers").(string)

	saslUsername := d.Get("sasl_username").(string)
	saslPassword := d.Get("sasl_password").(string)
	saslMechanism := d.Get("sasl_mechanism").(string)
	isSaslEnabled := saslUsername != "" && saslPassword != "" && saslMechanism != ""

	kafkaConnectUrl := d.Get("kafka_connects").(string)

	var diags diag.Diagnostics

	if bootstrapServers != "" {
		config := client.Config{
			BootstrapServers: []string{bootstrapServers},
			SaslMechanism:    saslMechanism,
			SaslPassword:     saslPassword,
			SaslUsername:     saslUsername,
			IsSaslEnabled:    isSaslEnabled,
		}

		kafkaConnectClient := &client.KafkaConnectCluster{}

		if kafkaConnectUrl != "" {
			kafkaConnectClient = client.NewKafkaConnectClient(kafkaConnectUrl)
		}

		cluster := client.NewKafkaCluster(bootstrapServers, config, *kafkaConnectClient)
		return cluster, diags
	}
	return nil, diags
}
