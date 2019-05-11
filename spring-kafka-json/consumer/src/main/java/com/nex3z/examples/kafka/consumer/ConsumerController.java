package com.nex3z.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller @RequestMapping("/consumer") @Slf4j
public class ConsumerController {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @PostMapping("/pause") @ResponseBody
    public String pause() {
        MessageListenerContainer container = registry.getListenerContainer(ConsumerListener.CONTAINER_ID);
        log.info("wakeup(): pausing consumer, container.getGroupId() = {}", container.getGroupId());
        container.pause();
        return "success";
    }

    @PostMapping("/resume") @ResponseBody
    public String resume() {
        log.info("resume(): resuming consumer");
        registry.getListenerContainer(ConsumerListener.CONTAINER_ID).resume();
        return "success";
    }

}
