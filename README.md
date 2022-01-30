# reactor-kafka-sandbox
Sandbox for reactor kafka

# Run Container Kafka
docker run -d -p 2181:2181 -p 9092:9092 -e ADVERTISED_HOST=127.0.0.1 -e NUM_PARTITIONS=1 johnnypark/kafka-zookeeper

# Create topic and run consumer
docker ps -> container_id

docker exec -ti {container_id} /bin/bash

cd /opt/kafka_2.13-2.6.0/bin/

./kafka-topics.sh --create --topic my-example-topic --replication-factor 1 --partitions 2 --zookeeper localhost:2181

./kafka-console-consumer.sh --topic my-example-topic --from-beginning --bootstrap-server localhost:9092