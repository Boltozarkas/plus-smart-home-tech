package ru.yandex.practicum.collector.service;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
@Slf4j
public class KafkaEventSender {

    @Value("${collector.topic.sensors}")
    private String sensorsTopic;

    @Value("${collector.topic.hubs}")
    private String hubsTopic;

    private final KafkaProducer<String, SpecificRecordBase> producer;

    public KafkaEventSender(KafkaProducer<String, SpecificRecordBase> producer) {
        this.producer = producer;
    }

    public void sendSensorEvent(String hubId, SpecificRecordBase event, Instant timestamp) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                sensorsTopic, null, timestamp.toEpochMilli(), hubId, event);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send sensor event to Kafka. Key: {}", hubId, exception);
            } else {
                log.info("Sensor event sent to topic {}: partition {}, offset {}, key: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), hubId);
            }
        });
    }

    public void sendHubEvent(String hubId, SpecificRecordBase event, Instant timestamp) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                hubsTopic, null, timestamp.toEpochMilli(), hubId, event);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send hub event to Kafka. Key: {}", hubId, exception);
            } else {
                log.info("Hub event sent to topic {}: partition {}, offset {}, key: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), hubId);
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Kafka producer...");
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
            log.info("Kafka producer closed successfully");
        } catch (Exception e) {
            log.error("Error closing Kafka producer", e);
        }
    }
}