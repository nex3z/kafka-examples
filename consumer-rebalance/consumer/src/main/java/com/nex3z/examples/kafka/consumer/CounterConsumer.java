package com.nex3z.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
public class CounterConsumer {

    private final String topic;

    private final Consumer<String, String> consumer;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public CounterConsumer(String servers, String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        this.topic = topic;

        this.consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        log.info("run(): started");
        try {
            consumer.subscribe(Collections.singletonList(topic), new RebalanceListener());

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (records.count() == 0) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    log.info("run(): received value = {}, partition = {}, offset = {}",
                            record.value(), record.partition(), record.offset());
                    offsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no meta data")
                    );
                }
                consumer.commitAsync(offsets, null);
            }
        } catch (WakeupException e) {
            log.info("run(): wake up", e);
        } catch (Exception e) {
            log.error("run(): ", e);
        } finally {
            try {
                consumer.commitSync(offsets);
                log.info("run(): committed offset before close {}", offsets);
            } finally {
                consumer.close();
                log.info("run(): consumer closed");
            }
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("onPartitionsRevoked(): committing offset {}", offsets);
            consumer.commitSync(offsets);
            offsets.clear();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("onPartitionsAssigned(): {}", partitions);
        }
    }
}
