#!/bin/bash
echo "Creating Kafka topics for Content Recommender..."

for TOPIC in watch-events click-events rating-events search-events; do
    docker exec -it content-recommender-kafka-1 kafka-topics \
        --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic $TOPIC \
        --partitions 4 \
        --replication-factor 1
    echo "Created: $TOPIC"
done

echo "All Kafka topics ready."