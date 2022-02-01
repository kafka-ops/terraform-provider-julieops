terraform {
  required_providers {
    julieops = {
      version = "0.3"
      source  = "purbon.com/com/julieops"
    }
  }
}

provider "julieops" {
  bootstrap_servers = "localhost:29092"
}

data "julieops_kafka_topic" "all" {
  name = "_schemas"
}

resource "julieops_kafka_topic" "custom_topic" {
  name = "foo"
  partitions = 1
  replication_factor = 1
  config = {
    "retention.ms": "42"
  }
}


output "all_topics" {
  value = data.julieops_kafka_topic.all
}

output "custom_topic" {
  value = julieops_kafka_topic.custom_topic
}