package com.nex3z.examples.kafka.consumer;

import com.nex3z.examples.kafka.data.CounterRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service @Slf4j
public class ConsumerListener {
    public static final String CONTAINER_ID = "container-1";

    @KafkaListener(topics = "${kafka.topic}", id = CONTAINER_ID)
    public void listen(CounterRecord record) {
        log.info("listen(): record = {}", record);
    }

}
