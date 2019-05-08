package com.nex3z.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

@Component @Slf4j
public class ConsumerRunner implements ApplicationRunner {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private Consumer<String, String> consumer;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("run(): started");
        try {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (records.count() == 0) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    log.info("run(): received value = {}, partition = {},", record.value(), record.partition());
                }
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("run(): wake up", e);
        } catch (Exception e) {
            log.info("run(): ", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
                log.info("run(): consumer closed");
            }
        }
    }
}
