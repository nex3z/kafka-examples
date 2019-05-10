package com.nex3z.examples.kafka.consumer;

import com.nex3z.examples.kafka.data.CounterRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Properties;

@Configuration
public class ConsumerConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String servers;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public Consumer<String, CounterRecord> consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>(CounterRecord.class, false));
    }

}
