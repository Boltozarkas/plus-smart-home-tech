package ru.yandex.practicum.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.service.AggregationService;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

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

    public void start() {
        try {
            consumer.subscribe(List.of(consumerTopic));
            log.info("Subscribed to topic: {}", consumerTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(1000));

                // Пропускаем итерацию, если нет записей
                if (records.isEmpty()) {
                    continue;
                }

                boolean allProcessed = true;
                ConsumerRecord<String, SpecificRecordBase> failedRecord = null;

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    try {
                        SensorEventAvro event = (SensorEventAvro) record.value();
                        Optional<SensorsSnapshotAvro> snapshot = aggregationService.updateState(event);

                        if (snapshot.isPresent()) {
                            SensorsSnapshotAvro snap = snapshot.get();
                            ProducerRecord<String, SpecificRecordBase> producerRecord =
                                    new ProducerRecord<>(producerTopic, null,
                                            snap.getTimestamp().toEpochMilli(), snap.getHubId(), snap);

                            // Синхронная отправка — ждём результат
                            Future<RecordMetadata> future = producer.send(producerRecord);
                            RecordMetadata metadata = future.get();

                            log.info("Snapshot sent to topic {}: hubId {}, partition {}, offset {}",
                                    producerTopic, snap.getHubId(), metadata.partition(), metadata.offset());
                        }
                    } catch (Exception e) {
                        log.error("Failed to process record from partition {}, offset {}",
                                record.partition(), record.offset(), e);
                        allProcessed = false;
                        failedRecord = record;
                        break; // Прерываем обработку batch'а
                    }
                }

                // Коммитим offset только если весь batch успешно обработан
                if (allProcessed) {
                    consumer.commitSync();
                    log.info("Offsets committed successfully");
                } else {
                    // Возвращаем position к необработанной записи
                    if (failedRecord != null) {
                        TopicPartition partition = new TopicPartition(
                                failedRecord.topic(), failedRecord.partition());
                        consumer.seek(partition, failedRecord.offset());
                        log.warn("Seek to offset {} for partition {} due to processing errors",
                                failedRecord.offset(), failedRecord.partition());
                    }
                }
            }

        } catch (WakeupException e) {
            log.info("Wakeup exception received, shutting down...");
        } catch (Exception e) {
            log.error("Error during aggregation", e);
        } finally {
            // Только освобождение ресурсов
            try {
                producer.flush();
            } catch (Exception e) {
                log.error("Error during producer flush", e);
            } finally {
                log.info("Closing consumer");
                consumer.close();
                log.info("Closing producer");
                producer.close();
            }
        }
    }

    /**
     * Метод для штатной остановки consumer'а.
     * Вызывается из другого потока (например, при завершении приложения).
     */
    public void shutdown() {
        consumer.wakeup();
    }
}