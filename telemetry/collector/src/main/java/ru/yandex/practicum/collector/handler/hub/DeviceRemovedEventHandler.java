package ru.yandex.practicum.collector.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.HubEventHandler;
import ru.yandex.practicum.collector.service.KafkaEventSender;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Instant;

@Component
public class DeviceRemovedEventHandler implements HubEventHandler {

    private final KafkaEventSender kafkaEventSender;

    public DeviceRemovedEventHandler(KafkaEventSender kafkaEventSender) {
        this.kafkaEventSender = kafkaEventSender;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {
        DeviceRemovedEventProto deviceRemoved = event.getDeviceRemoved();
        DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemoved.getId())
                .build();

        HubEventAvro avroEvent = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(payload)
                .build();

        kafkaEventSender.sendHubEvent(event.getHubId(), avroEvent, avroEvent.getTimestamp());
    }
}