package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventSender {

    @Value("${collector.topic.sensors}")
    private String sensorsTopic;

    @Value("${collector.topic.hubs}")
    private String hubsTopic;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public void sendSensorEvent(String key, byte[] event) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(sensorsTopic, key, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send sensor event to Kafka. Key: {}", key, ex);
                    } else {
                        log.info("Sensor event sent to topic {}: partition {}, offset {}, key: {}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                key);
                    }
                });
    }

    public void sendHubEvent(String key, byte[] event) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(hubsTopic, key, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send hub event to Kafka. Key: {}", key, ex);
                    } else {
                        log.info("Hub event sent to topic {}: partition {}, offset {}, key: {}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                key);
                    }
                });
    }
}