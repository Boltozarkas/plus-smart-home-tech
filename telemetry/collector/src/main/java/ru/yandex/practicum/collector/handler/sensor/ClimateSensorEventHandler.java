package ru.yandex.practicum.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.SensorEventHandler;
import ru.yandex.practicum.collector.service.KafkaEventSender;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Instant;

@Component
public class ClimateSensorEventHandler implements SensorEventHandler {

    private final KafkaEventSender kafkaEventSender;

    public ClimateSensorEventHandler(KafkaEventSender kafkaEventSender) {
        this.kafkaEventSender = kafkaEventSender;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        ClimateSensorProto sensor = event.getClimateSensor();
        ClimateSensorAvro payload = ClimateSensorAvro.newBuilder()
                .setTemperatureC(sensor.getTemperatureC())
                .setHumidity(sensor.getHumidity())
                .setCo2Level(sensor.getCo2Level())
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