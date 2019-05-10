package com.nex3z.examples.kafka.producer;

import com.nex3z.examples.kafka.data.CounterRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service @Slf4j
public class ProducerScheduler {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private Producer<String, CounterRecord> producer;

    private int count = 0;

    @Scheduled(fixedRate = 1000)
    public void produce() {
        int value = count++;
        log.info("produce(): sending value = {}", value);
        producer.send(new ProducerRecord<>(topic, new CounterRecord("name", count)));
    }

}
