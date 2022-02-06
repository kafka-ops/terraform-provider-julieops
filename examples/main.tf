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
}


resource "julieops_kafka_topic" "custom_topic" {
  name = "foo"
  partitions = 1
  replication_factor = 1
  config = {
    "retention.ms": "24"
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

resource "julieops_kafka_streams_acl" "kstreams" {
  project = "context.project"
  principal = "User:streams"
  read_topics = [ "foo" ]
  write_topics = [ "bar" ]
  metadata = {
    "foo" = "bar"
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