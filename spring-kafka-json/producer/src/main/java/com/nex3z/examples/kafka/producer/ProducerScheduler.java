package com.nex3z.examples.kafka.producer;

import com.nex3z.examples.kafka.data.CounterRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service @Slf4j
public class ProducerScheduler {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, CounterRecord> template;

    private int count = 0;

    @Scheduled(fixedRate = 1000)
    public void produce() {
        CounterRecord record = new CounterRecord("hello", count++);
        log.info("produce(): sending record = {}", record);
        template.send(topic, record);
    }

}
