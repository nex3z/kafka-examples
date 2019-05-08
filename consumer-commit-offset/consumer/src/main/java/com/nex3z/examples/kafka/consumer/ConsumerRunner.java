package com.nex3z.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component @Slf4j
public class ConsumerRunner implements ApplicationRunner {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private Consumer<String, String> consumer;

    private CommitCallback callback = new CommitCallback();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("run(): started");
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        int recordCount = 0;
        try {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (records.count() == 0) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    log.info("run(): received value = {}, partition = {},", record.value(), record.partition());
                    offsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no meta data")
                    );

                    if (++recordCount % 10 == 0) {
                        log.info("run(): committing offset {}", offsets);
                        consumer.commitAsync(offsets, callback);
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("run(): wake up", e);
        } catch (Exception e) {
            log.error("run(): ", e);
        } finally {
            try {
                consumer.commitSync(offsets);
            } finally {
                consumer.close();
                log.info("run(): consumer closed");
            }
        }
    }

    @Slf4j
    private static class CommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            log.info("onComplete(): commit complete");
        }
    }
}
