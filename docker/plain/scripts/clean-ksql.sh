#!/usr/bin/env bash

docker exec broker kafka-topics --bootstrap-server broker:9092 --list | grep ksql

TOPIC="_confluent-ksql-default__command_topic"

##
# --bootstrap-server <String: server(s)  REQUIRED: The server to connect to.
#   to use for bootstrapping>
# --command-config <String: command      A property file containing configs to
#   config property file path>             be passed to Admin Client.
# --help                                 Print usage information.
# --offset-json-file <String: Offset     REQUIRED: The JSON file with offset
#   json file path>                        per partition. The format to use is:
#                                        {"partitions":
#                                          [{"topic": "foo", "partition": 1,
#                                          "offset": 1}],
#                                         "version":1
#                                        }
# --version                              Display Kafka version.
##

# {"partitions": [{"topic": "mytest", "partition": 0, "offset": 90}], "version":1 }


docker exec broker kafka-delete-records --bootstrap-server broker:9092 \
   --offset-json-file my-file.json


docker exec broker kafka-console-consumer --bootstrap-server broker:9092 \
          --topic _confluent-ksql-default__command_topic \
          --from-beginning --property print.key=true --property print.offset=true
