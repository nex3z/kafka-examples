## Create topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic CounterAvroTopic
```

## Start console producer

```bash
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic CounterAvroTopic
```

## Start console consumer

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic CounterAvroTopic
```

## Wake up consumer client

```bash
curl -X POST http://localhost:8090/consumer/wakeup
```
