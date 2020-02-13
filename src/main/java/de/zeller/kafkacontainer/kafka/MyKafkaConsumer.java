package de.zeller.kafkacontainer.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class MyKafkaConsumer {
    public static final String SPRING_KAFKA_TOPIC_1 = "spring-kafka-topic-1";

    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;

    @Autowired
    public MyKafkaConsumer(final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
        this.containerFactory = containerFactory;
    }

    public void start(String key) {
        containerFactory.setRecordFilterStrategy(consumerRecord -> !consumerRecord.key().equals(key));

        ConcurrentMessageListenerContainer<String, String> container = containerFactory
                .createContainer(SPRING_KAFKA_TOPIC_1);
        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        container.setupMessageListener(new FilteringMessageListenerAdapter<String, String>(
                new AcknowledgingMessageListener<String, String>() {
                    @Override
                    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
                        System.out.println("new message: " + consumerRecord.value());
                        acknowledgment.acknowledge();
                    }
                },
                new RecordFilterStrategy<String, String>() {
                    @Override
                    public boolean filter(ConsumerRecord<String, String> consumerRecord) {
                        return !consumerRecord.key().equals(key);
                    }
                }
        ));
        container.start();
    }
}
