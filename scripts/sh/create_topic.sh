#/bin/bash

/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic $TOPIC --bootstrap-server $BOOTSTRAP_SERVER
echo "topic $TOPIC created successfully"