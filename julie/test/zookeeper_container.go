package test

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path/filepath"
)

const ZookeeperPortString = "2181"

func getPwd() string {
	dir, _ := os.Getwd()
	return dir
}

func AbsoluteMountPath(root string, relativePath string) string {
	path, _ := filepath.Abs(getPwd() + relativePath + root)
	return path
}

func SetupZookeeperEnv(ctx context.Context, versionTag string, mountPath string) (*zookeeperContainer, error) {

	saslConfig := "-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider -DrequireClientAuthScheme=sasl"
	zkOpts := fmt.Sprintf("-Djava.security.auth.login.config=/etc/kafka/zookeeper.sasl.jaas %s", saslConfig)
	env := map[string]string{
		"ZOOKEEPER_CLIENT_PORT": ZookeeperPortString,
		"ZOOKEEPER_TICK_TIME":   "2000",
		"KAFKA_OPTS":            zkOpts,
	}

	mounts := map[string]string{
		"/etc/kafka": mountPath,
	}

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-zookeeper:" + versionTag,
		ExposedPorts: []string{"2181/tcp"},
		Env:          env,
		BindMounts:   mounts,
		Name:         "zookeeper",
		Hostname:     "zookeeper",
		Networks: []string{
			"julie_ops-cli",
		},
		WaitingFor: wait.ForListeningPort(ZookeeperPortString),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, err
	}

	hostname := "zookeeper"
	uri := fmt.Sprintf("%s:%s", hostname, ZookeeperPortString)

	return &zookeeperContainer{Container: container, URI: uri}, nil
}
