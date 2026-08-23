package ru.yandex.practicum.analyzer.processor;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final KafkaConsumer<String, SpecificRecordBase> snapshotConsumer;
    private final ScenarioRepository scenarioRepository;

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    @Value("${kafka.consumer.snapshots.topic}")
    private String snapshotsTopic;

    public void start() {
        try {
            snapshotConsumer.subscribe(List.of(snapshotsTopic));
            log.info("SnapshotProcessor subscribed to: {}", snapshotsTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = snapshotConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    SensorsSnapshotAvro snapshot = (SensorsSnapshotAvro) record.value();
                    processSnapshot(snapshot);
                }
                snapshotConsumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        } finally {
            snapshotConsumer.close();
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());
        for (Scenario scenario : scenarios) {
            boolean allConditionsMet = checkConditions(scenario, snapshot);
            if (allConditionsMet) {
                executeActions(scenario, snapshot);
            }
        }
    }

    private boolean checkConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (ScenarioCondition scenarioCondition : scenario.getConditions()) {
            Sensor sensor = scenarioCondition.getSensor();
            Condition condition = scenarioCondition.getCondition();
            SensorStateAvro state = snapshot.getSensorsState().get(sensor.getId());

            if (state == null) {
                return false;
            }

            if (!evaluateCondition(condition, state)) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateCondition(Condition condition, SensorStateAvro state) {
        Object data = state.getData();
        int sensorValue = extractValue(data, condition.getType());
        int conditionValue = condition.getValue() != null ? condition.getValue() : 0;

        return switch (condition.getOperation()) {
            case "EQUALS" -> sensorValue == conditionValue;
            case "GREATER_THAN" -> sensorValue > conditionValue;
            case "LOWER_THAN" -> sensorValue < conditionValue;
            default -> false;
        };
    }

    private int extractValue(Object data, String type) {
        // Упрощённо — в реальном коде нужно проверять тип данных
        if (data instanceof ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro climate) {
            return switch (type) {
                case "TEMPERATURE" -> climate.getTemperatureC();
                case "HUMIDITY" -> climate.getHumidity();
                case "CO2LEVEL" -> climate.getCo2Level();
                default -> 0;
            };
        } else if (data instanceof ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro light) {
            return switch (type) {
                case "LUMINOSITY" -> light.getLuminosity();
                default -> 0;
            };
        } else if (data instanceof ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro motion) {
            return switch (type) {
                case "MOTION" -> motion.getMotion() ? 1 : 0;
                default -> 0;
            };
        } else if (data instanceof ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro sw) {
            return switch (type) {
                case "SWITCH" -> sw.getState() ? 1 : 0;
                default -> 0;
            };
        } else if (data instanceof ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro temp) {
            return switch (type) {
                case "TEMPERATURE" -> temp.getTemperatureC();
                default -> 0;
            };
        }
        return 0;
    }

    private void executeActions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (ScenarioAction scenarioAction : scenario.getActions()) {
            Sensor sensor = scenarioAction.getSensor();
            Action action = scenarioAction.getAction();

            DeviceActionProto deviceAction = DeviceActionProto.newBuilder()
                    .setSensorId(sensor.getId())
                    .setType(ActionTypeProto.valueOf(action.getType()))
                    .setValue(action.getValue() != null ? action.getValue() : 0)
                    .build();

            Instant instant = Instant.now();
            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenario.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(deviceAction)
                    .setTimestamp(Timestamp.newBuilder()
                            .setSeconds(instant.getEpochSecond())
                            .setNanos(instant.getNano())
                            .build())
                    .build();

            hubRouterClient.handleDeviceAction(request);
            log.info("Action executed: scenario={}, sensor={}, action={}",
                    scenario.getName(), sensor.getId(), action.getType());
        }
    }
}