package com.nex3z.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Controller @RequestMapping("/consumer") @Slf4j
public class ConsumerController {

    @Value("${kafka.bootstrap-servers}")
    private String servers;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    private List<CounterConsumer> consumers = Collections.synchronizedList(new ArrayList<>());

    @PostMapping("/add") @ResponseBody
    public String addConsumer() {
        log.info("wakeup(): adding consumer");
        CounterConsumer consumer = new CounterConsumer(servers, groupId, topic);
        new Thread("Consumer-" + consumers.size()) {
            @Override
            public void run() {
                consumer.run();
            }
        }.start();
        consumers.add(consumer);
        return "added consumer " + (consumers.size() - 1);
    }

    @PostMapping("/wakeup/{id}") @ResponseBody
    public String wakeup(@PathVariable Integer id) {
        log.info("wakeup(): waking up consumer");
        if (consumers.size() > id) {
            consumers.get(id).wakeup();
            return "waked up consumer " + id;
        } else {
            return "failed to wake up consumer " + id;
        }
    }
}
