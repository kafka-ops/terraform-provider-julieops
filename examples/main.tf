terraform {
  required_providers {
    julieops = {
      version = "0.2"
      source  = "purbon.com/com/julieops"
    }
  }
}

provider "julieops" {
  bootstrap_servers = ["localhost:9092"]
}

data "julieops_kafka_topic" "all" {
  name = "default_ksql_processing_log"
  partitions = 1
  replication_factor = 3

}

output "all_topics" {
  value = data.julieops_kafka_topic.all
}