package ru.yandex.practicum.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class AggregationService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        // Получаем или создаём снапшот для хаба
        SensorsSnapshotAvro snapshot = snapshots.get(event.getHubId());
        if (snapshot == null) {
            snapshot = SensorsSnapshotAvro.newBuilder()
                    .setHubId(event.getHubId())
                    .setTimestamp(event.getTimestamp())
                    .setSensorsState(new HashMap<>())
                    .build();
        }

        // Проверяем, есть ли данные для конкретного датчика
        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());
        if (oldState != null) {
            // Если старые данные новее — игнорируем
            if (oldState.getTimestamp().isAfter(event.getTimestamp())) {
                return Optional.empty();
            }
            // Если данные не изменились — игнорируем
            if (oldState.getData() != null && oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        // Обновляем данные датчика
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        Map<String, SensorStateAvro> sensorsState = new HashMap<>(snapshot.getSensorsState());
        sensorsState.put(event.getId(), newState);

        SensorsSnapshotAvro updatedSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(sensorsState)
                .build();

        snapshots.put(event.getHubId(), updatedSnapshot);

        log.info("Snapshot updated for hub {}: sensor {} changed", event.getHubId(), event.getId());
        return Optional.of(updatedSnapshot);
    }
}