package com.nex3z.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller @RequestMapping("/consumer") @Slf4j
public class ConsumerController {

    @Autowired
    private Consumer<String, String> consumer;

    @PostMapping("/wakeup") @ResponseBody
    public String wakeup() {
        log.info("wakeup(): waking up consumer");
        consumer.wakeup();
        return "success";
    }

}
