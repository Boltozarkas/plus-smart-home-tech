package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.model.hub.HubEvent;
import ru.yandex.practicum.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.collector.service.KafkaEventSender;

@RestController
@RequiredArgsConstructor
@Slf4j
public class EventController {

    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;
    private final KafkaEventSender kafkaEventSender;

    @PostMapping("/events/sensors")
    public ResponseEntity<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: type={}, hubId={}, id={}",
                event.getType(), event.getHubId(), event.getId());
        kafkaEventSender.sendSensorEvent(
                event.getHubId(),
                sensorEventMapper.toAvro(event),
                event.getTimestamp()
        );
        return ResponseEntity.ok().build();
    }

    @PostMapping("/events/hubs")
    public ResponseEntity<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Received hub event: type={}, hubId={}",
                event.getType(), event.getHubId());
        kafkaEventSender.sendHubEvent(
                event.getHubId(),
                hubEventMapper.toAvro(event),
                event.getTimestamp()
        );
        return ResponseEntity.ok().build();
    }
}