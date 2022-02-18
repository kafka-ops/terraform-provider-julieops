terraform {
  required_providers {
    julieops = {
      version = "0.3"
      source  = "purbon.com/com/julieops"
    }
  }
}

provider "julieops" {
  bootstrap_servers = "localhost:9092"
  sasl_username = "kafka"
  sasl_password = "kafka"
  sasl_mechanism = "plain"
  kafka_connects = "http://localhost:18083"
}


resource "julieops_kafka_topic" "custom_topic" {
  name = "foo"
  partitions = 1
  replication_factor = 1
  config = {
    "retention.ms" =  "24"
  }
}

resource "julieops_kafka_consumer_acl" "consumer" {
  project = "context.project"
  principal = "User:bar"
  group = "*"
  metadata = {
    "foo" = "bar"
  }
}

resource "julieops_kafka_streams_acl" "kstreams_v1" {
  project = "context.project"
  principal = "User:streams_v1"
  read_topics = [ "foo" ]
  write_topics = [ "bar" ]
  metadata = {
    "foo" = "bar"
  }
}

resource "julieops_kafka_streams_acl" "kstreams_v2" {
  project = "context.project"
  principal = "User:streams_v2"
  read_topics = [ "foo", "zet" ]
  write_topics = [ "bar" ]
  metadata = {
    "foo" = "bar"
  }
}

resource "julieops_kafka_connect_acl" "connect" {
  principal = "User:connect"
  read_topics = [ "foo" ]
  write_topics = [ "bar" ]
  metadata = {
    "foo" = "bar"
  }
}


resource "julieops_kafka_connector" "test" {
  name = "foo"
  config = {
    "connector.class" =                 "io.confluent.kafka.connect.datagen.DatagenConnector"
    "kafka.topic" =                     "pageviews"
    "quickstart" =                      "pageviews"
    "key.converter" =                   "org.apache.kafka.connect.storage.StringConverter"
    "value.converter" =                 "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable" =  "false"
    "max.interval" =                    100
    "iterations" =                      10000000
    "tasks.max" =                       "1"
  }
}

data "julieops_kafka_topic" "schemas" {
  name = "_schemas"
}

output "all_topics" {
  value = data.julieops_kafka_topic.schemas
}

output "custom_topic" {
  value = julieops_kafka_topic.custom_topic
}