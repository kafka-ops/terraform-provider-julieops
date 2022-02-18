package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type KafkaConnectCluster struct {
	Url    string
	Client http.Client
}

type ClusterInfoResponse struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterId string `json:"kafka_cluster_id"`
}

type ConnectorsResponse struct {
	Connectors []string `json:""`
}

func NewKafkaConnectClient(url string) *KafkaConnectCluster {

	defaultTimeout, _ := time.ParseDuration("30s")

	client := http.Client{
		Timeout: defaultTimeout,
	}

	return &KafkaConnectCluster{
		Url:    url,
		Client: client,
	}
}

func (kc KafkaConnectCluster) doGetRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Accept", "application/json")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return kc.Client.Do(req)
}

func (kc KafkaConnectCluster) doDeleteRequest(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return kc.Client.Do(req)
}

func (kc KafkaConnectCluster) doPostRequest(url string, bodyData []byte) (*http.Response, error) {

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(bodyData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return kc.Client.Do(req)
}

func (kc KafkaConnectCluster) doPutRequest(url string, bodyData []byte) (*http.Response, error) {

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(bodyData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return kc.Client.Do(req)
}

func (kc KafkaConnectCluster) GetClusterInfo() (*ClusterInfoResponse, error) {

	response, err := kc.doGetRequest(kc.Url)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer response.Body.Close()
	var clusterInfoResponse ClusterInfoResponse
	if err := json.NewDecoder(response.Body).Decode(&clusterInfoResponse); err != nil {
		log.Println(err)
		return nil, err
	}

	return &clusterInfoResponse, nil
}

func (kc KafkaConnectCluster) GetConnectors() (*ConnectorsResponse, error) {
	connectorsUrl := kc.Url + "/connectors"
	response, err := kc.doGetRequest(connectorsUrl)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	var connectors ConnectorsResponse
	if err := json.Unmarshal(body, &connectors); err != nil {
		log.Println(err)
		return nil, err
	}
	return &connectors, nil
}

func (kc KafkaConnectCluster) GetConnector(name string) (*GetConnectorResponse, error) {
	connectorsUrl := kc.Url + "/connectors/" + name
	response, err := kc.doGetRequest(connectorsUrl)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer response.Body.Close()

	var getConnectorResponse GetConnectorResponse

	if err := json.NewDecoder(response.Body).Decode(&getConnectorResponse); err != nil {
		log.Println(err)
		return nil, err
	}

	return &getConnectorResponse, nil
}

type ConnectorCreateRequest struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

type GetConnectorResponse struct {
	Name   string              `json:"name"`
	Config map[string]string   `json:"config"`
	Tasks  []ConnectorTaskInfo `json:"tasks"`
}

type ConnectorCreateResponse struct {
	Name   string              `json:"name"`
	Config map[string]string   `json:"config"`
	Tasks  []ConnectorTaskInfo `json:"tasks"`
}

type ConnectorTaskInfo struct {
	Connector string `json:"connector"`
	Task      int64  `json:"task"`
}

func (kc KafkaConnectCluster) AddConnector(c ConnectorCreateRequest) (*ConnectorCreateResponse, error) {
	return kc.AddOrUpdateConnector(c)
}

func (kc KafkaConnectCluster) AddOrUpdateConnector(c ConnectorCreateRequest) (*ConnectorCreateResponse, error) {
	connectorsUrl := kc.Url + "/connectors/" + c.Name + "/config"
	body, err := json.Marshal(c.Config)
	if err != nil {
		return nil, err
	}
	response, err := kc.doPutRequest(connectorsUrl, body)

	if response.StatusCode == 409 {
		errorCode := fmt.Errorf("a rebalance is in place, please check your Kafka Connect cluster")
		return nil, errorCode
	}
	if response.StatusCode > 400 {
		errorCode := fmt.Errorf("something happened while trying to create a connector, response Code = %d", response.StatusCode)
		return nil, errorCode
	}

	defer response.Body.Close()
	var connectorCreateResponse ConnectorCreateResponse
	if err := json.NewDecoder(response.Body).Decode(&connectorCreateResponse); err != nil {
		log.Println(err)
		return nil, err
	}

	return &connectorCreateResponse, nil
}

func (kc KafkaConnectCluster) DeleteConnector(name string) error {
	connectorsUrl := kc.Url + "/connectors/" + name
	response, err := kc.doDeleteRequest(connectorsUrl)
	if err != nil {
		log.Println(err)
		return err
	}
	defer response.Body.Close()

	if response.StatusCode == 409 {
		errorCode := fmt.Errorf("a rebalance is in place, please check your Kafka Connect cluster")
		return errorCode
	}
	if response.StatusCode > 400 {
		errorCode := fmt.Errorf("something happened while trying to create a connector, response Code = %d", response.StatusCode)
		return errorCode
	}

	return nil
}

func (c *ConnectorsResponse) UnmarshalJSON(p []byte) error {

	var tmp []string

	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}

	c.Connectors = tmp
	return nil
}
