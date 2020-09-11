package com.prioritypoc.springkafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class MessageProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${online.topic.name}")
    private String onlineTopicName;

    @Value(value = "${batch.topic.name}")
    private String batchTopicName;

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    public void sendToOnlineTopic(@lombok.NonNull String message) {

         // actual functionality goes here. Code here to publish
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(onlineTopicName, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.debug("Sent to online topic, message=[" + message + "] with offset=[" + result.getRecordMetadata()
                        .offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.warn("Unable to send message =[" + message + "] to online topic due to : " + ex.getMessage());
            }
        });
    }

    public void sendToBatchTopic(@lombok.NonNull String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(batchTopicName, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.debug("Sent to batch topic, message=[" + message + "] with offset=[" + result.getRecordMetadata()
                        .offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.warn("Unable to send message =[" + message + "] to batch topic due to : " + ex.getMessage());
            }
        });
    }

}
