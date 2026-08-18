package ru.yandex.practicum.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.SensorEventHandler;
import ru.yandex.practicum.collector.service.KafkaEventSender;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Instant;

@Component
public class LightSensorEventHandler implements SensorEventHandler {

    private final KafkaEventSender kafkaEventSender;

    public LightSensorEventHandler(KafkaEventSender kafkaEventSender) {
        this.kafkaEventSender = kafkaEventSender;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        LightSensorProto sensor = event.getLightSensor();
        LightSensorAvro payload = LightSensorAvro.newBuilder()
                .setLinkQuality(sensor.getLinkQuality())
                .setLuminosity(sensor.getLuminosity())
                .build();

        SensorEventAvro avroEvent = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(payload)
                .build();

        kafkaEventSender.sendSensorEvent(event.getHubId(), avroEvent, avroEvent.getTimestamp());
    }
}