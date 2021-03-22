package org.example;

import com.ibm.gbs.schema.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
public class MessageConsumer {

    @KafkaListener(containerFactory = "kafkaListenerContainerFactory",
            topics = "${kafka.topic.name}",
            id = "listener",
            groupId = "${kafka.topic.consumer-group}")
    public void listener(Acknowledgment acknowledgment, ConsumerRecord<Long, Customer> record) {
        log.info("Processing Message Key = {},  Value= {} ", record.key(),record.value().getCustomerId());
        acknowledgment.acknowledge();
    }
}
