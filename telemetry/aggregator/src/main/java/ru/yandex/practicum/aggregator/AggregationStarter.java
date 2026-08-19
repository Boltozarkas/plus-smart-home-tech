package ru.yandex.practicum.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.service.AggregationService;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final KafkaConsumer<String, SpecificRecordBase> consumer;
    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final AggregationService aggregationService;

    @Value("${kafka.consumer.topic}")
    private String consumerTopic;

    @Value("${kafka.producer.topic}")
    private String producerTopic;

    private volatile boolean running = true;

    public void start() {
        try {
            consumer.subscribe(Collections.singletonList(consumerTopic));
            log.info("Subscribed to topic: {}", consumerTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    SensorEventAvro event = (SensorEventAvro) record.value();
                    Optional<SensorsSnapshotAvro> snapshot = aggregationService.updateState(event);

                    if (snapshot.isPresent()) {
                        SensorsSnapshotAvro snap = snapshot.get();
                        ProducerRecord<String, SpecificRecordBase> producerRecord =
                                new ProducerRecord<>(producerTopic, null,
                                        snap.getTimestamp().toEpochMilli(), snap.getHubId(), snap);

                        producer.send(producerRecord);
                        log.info("Snapshot sent to topic {}: hubId {}", producerTopic, snap.getHubId());
                    }
                }

                // Фиксируем смещение
                consumer.commitSync();
            }

        } catch (WakeupException e) {
            log.info("Wakeup exception received, shutting down...");
        } catch (Exception e) {
            log.error("Error during aggregation", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Closing consumer");
                consumer.close();
                log.info("Closing producer");
                producer.close();
            }
        }
    }
}