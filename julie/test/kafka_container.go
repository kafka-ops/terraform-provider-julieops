package test

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

const KafkaPort = 9092
const KafkaPortString = "9092"
const KafkaInternalPort = 9093
const KafkaConnectPortString = "18083"
const SchemaRegistryPortString = "8081"

const InternalListenerName = "BROKER"
const VersionTag = "6.1.0"

type DeploymentContainer struct {
	Container testcontainers.Container
	URI       string
}
type kafkaContainer DeploymentContainer
type zookeeperContainer DeploymentContainer
type kafkaConnectContainer DeploymentContainer
type schemaRegistryContainer DeploymentContainer

type ContainerSetup struct {
	ZkContainer *zookeeperContainer
	AkContainer *kafkaContainer
	KcContainer *kafkaConnectContainer
	SrContainer *schemaRegistryContainer
}

type ContainersSetupConfig struct {
	RootPath             string
	EnableKafkaConnect   bool
	EnableSchemaRegistry bool
}

func SetupDocker(ctx context.Context, config ContainersSetupConfig, t *testing.T) (*ContainerSetup, func(c context.Context)) {
	config.RootPath = AbsoluteMountPath("docker/res/", "/../")
	return SetupDockerWithPath(ctx, config, t)
}

func SetupDockerWithPath(ctx context.Context, config ContainersSetupConfig, t *testing.T) (*ContainerSetup, func(c context.Context)) {
	setup, close, err := BeforeEach(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	return setup, close
}

func BeforeEach(ctx context.Context, config ContainersSetupConfig) (*ContainerSetup, func(c context.Context), error) {

	containerConfig := DockerContainerConfig{
		Dockerfile: config.RootPath + "/kafka-connect/Dockerfile",
		VersionTag: VersionTag,
	}
	setup := ContainerSetup{}

	zkMountPath := config.RootPath + "/zookeeper"
	akMountPath := config.RootPath + "/kafka"

	zkContainer, err := SetupZookeeperEnv(ctx, VersionTag, zkMountPath)
	if err != nil {
		return nil, nil, err
	}
	containerConfig.ZookeeperUri = zkContainer.URI
	setup.ZkContainer = zkContainer

	akContainer, err := SetupKafkaEnv(ctx, zkContainer.URI, VersionTag, akMountPath)
	if err != nil {
		return nil, nil, err
	}
	containerConfig.KafkaUri = akContainer.URI
	setup.AkContainer = akContainer

	if config.EnableSchemaRegistry {

		srContainer, err := SetupSchemaRegistry(ctx, containerConfig)
		if err != nil {
			return nil, nil, err
		}
		containerConfig.SchemaRegistryUri = srContainer.URI
		setup.SrContainer = srContainer
	}

	if config.EnableKafkaConnect {
		containerConfig.MountPath = config.RootPath + "/kafka-connect"
		kcContainer, err := SetupKafkaConnect(ctx, containerConfig)
		if err != nil {
			return nil, nil, err
		}
		setup.KcContainer = kcContainer
	}

	return &setup, func(c context.Context) {
		if setup.KcContainer != nil {
			setup.KcContainer.Container.Terminate(c)
		}
		if setup.SrContainer != nil {
			setup.SrContainer.Container.Terminate(c)
		}
		if setup.AkContainer != nil {
			setup.AkContainer.Container.Terminate(c)
		}
		if setup.ZkContainer != nil {
			_, err := waitUntilTerminated(c, setup.ZkContainer.Container)
			if err != nil {
				fmt.Printf("Error: %s", err)
			}
		}
	}, nil
}

func waitUntilTerminated(ctx context.Context, container testcontainers.Container) (bool, error) {
	err := container.Terminate(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

func SetupKafkaEnv(ctx context.Context, zookeeperUri string, versionTag string, mountPath string) (*kafkaContainer, error) {

	env := map[string]string{
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           securityProtocol(),
		"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
		"KAFKA_ADVERTISED_LISTENERS":                     advertiseListeners(),
		"KAFKA_ZOOKEEPER_CONNECT":                        zookeeperUri,
		"KAFKA_BROKER_ID":                                "1",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
		"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		"KAFKA_LOG_FLUSH_INTERVAL_MESSAGES":              "9223372036854775807",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		"CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS":      "1",
		"CONFLUENT_SUPPORT_METRICS_ENABLE":               "false",
		"CONFLUENT_SUPPORT_CUSTOMER_ID":                  "anonymous",
		"KAFKA_SASL_ENABLED_MECHANISMS":                  "PLAIN",
		"KAFKA_AUTHORIZER_CLASS_NAME":                    "kafka.security.auth.SimpleAclAuthorizer",
		"KAFKA_MECHANISMS_INTER_BROKER_PROTOCOL":         "SASL_PLAINTEXT",
		"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL":     "PLAIN",
		"KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND":           "true",
		"KAFKA_SUPER_USERS":                              "User:kafka;User:ANONYMOUS",
		"KAFKA_OPTS":                                     "-Djava.security.auth.login.config=/etc/kafka/kafka_server.jaas",
		"KAFKA_LOG4J_LOGGERS":                            "kafka.authorizer.logger=DEBUG",
	}

	mounts := map[string]string{
		"/etc/kafka": mountPath,
	}

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:" + versionTag,
		ExposedPorts: []string{"9092:9092"},
		Env:          env,
		BindMounts:   mounts,
		Name:         "kafka",
		Hostname:     "kafka",
		Networks: []string{
			"julie_ops-cli",
		},
		WaitingFor: wait.ForListeningPort(KafkaPortString),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("%s:%s", ip, KafkaPortString)

	return &kafkaContainer{Container: container, URI: uri}, nil
}

func securityProtocol() string {
	return fmt.Sprintf("PLAINTEXT:PLAINTEXT,%s:SASL_PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT", InternalListenerName)
}

func listeners() string {
	return fmt.Sprintf("PLAINTEXT://kafka:29092,%s://localhost:%d,SASL_PLAINTEXT://127.0.0.1:%d", InternalListenerName, KafkaPort, KafkaInternalPort)

}

func advertiseListeners() string {
	return fmt.Sprintf("PLAINTEXT://kafka:29092,%s://localhost:%d,SASL_PLAINTEXT://kafka:29093", InternalListenerName, KafkaPort)
}

type DockerContainerConfig struct {
	KafkaUri          string
	ZookeeperUri      string
	SchemaRegistryUri string
	VersionTag        string
	MountPath         string
	Dockerfile        string
}

func SetupSchemaRegistry(ctx context.Context, config DockerContainerConfig) (*schemaRegistryContainer, error) {

	env := map[string]string{
		"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
		"SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL":    config.ZookeeperUri,
		"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "kafka:29092",
	}

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-schema-registry:" + config.VersionTag,
		ExposedPorts: []string{SchemaRegistryPortString + ":" + SchemaRegistryPortString},
		Env:          env,
		Name:         "schema-registry",
		Hostname:     "schema-registry",
		Networks: []string{
			"julie_ops-cli",
		},
		WaitingFor: wait.ForListeningPort(SchemaRegistryPortString),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("http://%s:%s", ip, SchemaRegistryPortString)

	return &schemaRegistryContainer{
		Container: container,
		URI:       uri,
	}, nil
}

func SetupKafkaConnect(ctx context.Context, config DockerContainerConfig) (*kafkaConnectContainer, error) {
	env := map[string]string{
		"CUB_CLASSPATH":                               "/usr/share/java/confluent-security/connect/*:/usr/share/java/kafka/*:/usr/share/java/cp-base-new/*",
		"CLASSPATH":                                   "/usr/share/java/kafka-connect-replicator/*:/usr/share/java/monitoring-interceptors/*",
		"CONNECT_BOOTSTRAP_SERVERS":                   "kafka:29092",
		"CONNECT_REST_PORT":                           KafkaConnectPortString,
		"CONNECT_GROUP_ID":                            "kafka-connect",
		"CONNECT_CONFIG_STORAGE_TOPIC":                "_kafka-connect-configs",
		"CONNECT_OFFSET_STORAGE_TOPIC":                "_kafka-connect-offsets",
		"CONNECT_STATUS_STORAGE_TOPIC":                "_kafka-connect-status",
		"CONNECT_KEY_CONVERTER":                       "io.confluent.connect.avro.AvroConverter",
		"CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL":   config.SchemaRegistryUri,
		"CONNECT_VALUE_CONVERTER":                     "io.confluent.connect.avro.AvroConverter",
		"CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL": config.SchemaRegistryUri,
		"CONNECT_LOG4J_ROOT_LOGLEVEL":                 "INFO",
		"CONNECT_LOG4J_LOGGERS":                       "org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR",
		"CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR":   "1",
		"CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR":   "1",
		"CONNECT_STATUS_STORAGE_REPLICATION_FACTOR":   "1",
		"CONNECT_REST_ADVERTISED_HOST_NAME":           "kafka-connect",
		"CONNECT_PLUGIN_PATH":                         "/usr/share/confluent-hub-components",
	}

	mounts := map[string]string{
		"/etc/kafka": config.MountPath,
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context: config.MountPath,
		},
		ExposedPorts: []string{KafkaConnectPortString + ":" + KafkaConnectPortString},
		Env:          env,
		BindMounts:   mounts,
		Name:         "kafka-connect",
		Hostname:     "kafka-connect",
		Networks: []string{
			"julie_ops-cli",
		},
		WaitingFor: wait.ForLog("Kafka Connect started (org.apache.kafka.connect.runtime.Connect)"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	uri := fmt.Sprintf("http://%s:%s", ip, KafkaConnectPortString)

	return &kafkaConnectContainer{
		Container: container,
		URI:       uri,
	}, nil
}
