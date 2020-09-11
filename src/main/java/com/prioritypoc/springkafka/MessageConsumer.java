package com.prioritypoc.springkafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import java.util.concurrent.TimeUnit;

@Service
public class MessageConsumer {

    @Value(value = "${online.topic.name}")
    private String onlineTopic;

    @Value(value = "${batch.topic.name}")
    private String batchTopic;

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    /**
     * Process messages sent to online topic. It also pauses batch processing before starting online processing
     * @param message
     * @param receivedPartition
     * @param receivedTopic
     */
    @KafkaListener(id="online",
                    topics = "${online.topic.name}",
                    containerFactory = "kafkaOnlineListenerContainerFactory",
                    groupId = "consumerGroup1")
    public void consumeOnlineMessage(@lombok.NonNull @Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int receivedPartition,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
                                    Acknowledgment acknowledgment) {

        //actual functionality goes here.
        registry.getListenerContainer("batch").pause();

        LOGGER.info("Processing record from online topic: " + message +" , from partition: " + receivedPartition);
        acknowledgment.acknowledge();

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Listen for idle time of online topic. If idle for the configured time of 5 seconds,
     * (refer KafkaConsumerConfig constructor for the value) then this event gets triggered
     * @param listenerContainerIdleEvent
     */
    @EventListener(condition = "event.listenerId.startsWith('online')")
    public void listenToOnlineEvent(ListenerContainerIdleEvent listenerContainerIdleEvent){
        registry.getListenerContainer("batch").resume();
    }

    /**
     * Processes messages sent to batch topic. This is controlled by EventListener (listenToOnlineEvent)
     * and kafkaListener (consumeOnlineMessage). But, note that first message always gets triggered no matter what
     * in order to start the container.
     * Make sure this consumer is part of the same consumer group that online processing consumer belong to.
     * @param message
     * @param receivedPartition
     * @param receivedTopic
     */
    @KafkaListener(id="batch",
                    topics = "${batch.topic.name}",
                    containerFactory = "kafkaBatchListenerContainerFactory",
                    groupId = "consumerGroup1")
    public void consumeBatchMessage(@lombok.NonNull @Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int receivedPartition,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
                                    Acknowledgment acknowledgment) {

        //actual functionality goes here.
        LOGGER.info("Processing record from batch topic: " + message +" from partition "+receivedPartition);
        acknowledgment.acknowledge();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
