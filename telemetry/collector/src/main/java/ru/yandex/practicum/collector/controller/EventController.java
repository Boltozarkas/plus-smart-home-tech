package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.collector.service.KafkaEventSender;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@RestController
public class EventController {

    private static final Logger log = LoggerFactory.getLogger(EventController.class);

    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;
    private final KafkaEventSender kafkaEventSender;

    public EventController(SensorEventMapper sensorEventMapper,
                           HubEventMapper hubEventMapper,
                           KafkaEventSender kafkaEventSender) {
        this.sensorEventMapper = sensorEventMapper;
        this.hubEventMapper = hubEventMapper;
        this.kafkaEventSender = kafkaEventSender;
    }

    @PostMapping("/events/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: type={}, hubId={}, id={}",
                event.getType(), event.getHubId(), event.getId());

        SpecificRecordBase avroEvent = sensorEventMapper.toAvro(event);
        byte[] serializedEvent = serializeAvro(avroEvent);
        kafkaEventSender.sendSensorEvent(event.getHubId(), serializedEvent);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/events/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Received hub event: type={}, hubId={}",
                event.getType(), event.getHubId());

        HubEventAvro avroEvent = hubEventMapper.toAvro(event);
        byte[] serializedEvent = serializeAvro(avroEvent);
        kafkaEventSender.sendHubEvent(event.getHubId(), serializedEvent);

        return ResponseEntity.ok().build();
    }

    private byte[] serializeAvro(SpecificRecordBase record) {
        try {
            DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(record.getSchema());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(record, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro event", e);
        }
    }
}