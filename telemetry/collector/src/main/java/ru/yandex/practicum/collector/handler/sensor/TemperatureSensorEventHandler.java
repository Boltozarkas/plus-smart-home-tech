package ru.yandex.practicum.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.SensorEventHandler;
import ru.yandex.practicum.collector.service.KafkaEventSender;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;

@Component
public class TemperatureSensorEventHandler implements SensorEventHandler {

    private final KafkaEventSender kafkaEventSender;

    public TemperatureSensorEventHandler(KafkaEventSender kafkaEventSender) {
        this.kafkaEventSender = kafkaEventSender;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        TemperatureSensorProto sensor = event.getTemperatureSensor();
        TemperatureSensorAvro payload = TemperatureSensorAvro.newBuilder()
                .setTemperatureC(sensor.getTemperatureC())
                .setTemperatureF(sensor.getTemperatureF())
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