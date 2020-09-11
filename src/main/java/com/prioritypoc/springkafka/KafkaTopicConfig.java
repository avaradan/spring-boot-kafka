package com.prioritypoc.springkafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

   @Value(value = "${online.topic.name}")
    private String onlineTopicName;

    @Value(value = "${batch.topic.name}")
    private String batchTopicName;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic online() {
        return new NewTopic(onlineTopicName, 3, (short) 3); //topic name, number of partitions, replication factor
    }

    @Bean
    public NewTopic batch() {
        return new NewTopic(batchTopicName, 3, (short) 3); //topic name, number of partitions, replication factor
    }

}