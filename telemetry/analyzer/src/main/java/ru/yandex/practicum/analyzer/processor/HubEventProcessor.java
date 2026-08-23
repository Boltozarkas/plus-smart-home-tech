package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, SpecificRecordBase> hubEventConsumer;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Value("${kafka.consumer.hub-events.topic}")
    private String hubEventsTopic;

    @Override
    public void run() {
        try {
            hubEventConsumer.subscribe(List.of(hubEventsTopic));
            log.info("HubEventProcessor subscribed to: {}", hubEventsTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = hubEventConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    HubEventAvro event = (HubEventAvro) record.value();
                    processHubEvent(event);
                }
                hubEventConsumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            hubEventConsumer.close();
        }
    }

    private void processHubEvent(HubEventAvro event) {
        Object payload = event.getPayload();
        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            Sensor sensor = Sensor.builder()
                    .id(deviceAdded.getId())
                    .hubId(event.getHubId())
                    .build();
            sensorRepository.save(sensor);
            log.info("Sensor added: {}", sensor);
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            sensorRepository.deleteById(deviceRemoved.getId());
            log.info("Sensor removed: {}", deviceRemoved.getId());
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            saveScenario(event.getHubId(), scenarioAdded);
            log.info("Scenario added: {}", scenarioAdded.getName());
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            scenarioRepository.findByHubIdAndName(event.getHubId(), scenarioRemoved.getName())
                    .ifPresent(scenarioRepository::delete);
            log.info("Scenario removed: {}", scenarioRemoved.getName());
        }
    }

    private void saveScenario(String hubId, ScenarioAddedEventAvro scenarioAdded) {
        Scenario scenario = Scenario.builder()
                .hubId(hubId)
                .name(scenarioAdded.getName())
                .build();
        scenario = scenarioRepository.save(scenario);

        for (ScenarioConditionAvro conditionAvro : scenarioAdded.getConditions()) {
            Condition condition = Condition.builder()
                    .type(conditionAvro.getType().name())
                    .operation(conditionAvro.getOperation().name())
                    .value(convertValue(conditionAvro.getValue()))
                    .build();
            condition = conditionRepository.save(condition);

            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Sensor not found: " + conditionAvro.getSensorId()));

            ScenarioConditionId id = new ScenarioConditionId(scenario.getId(), sensor.getId(), condition.getId());
            ScenarioCondition scenarioCondition = ScenarioCondition.builder()
                    .id(id)
                    .scenario(scenario)
                    .sensor(sensor)
                    .condition(condition)
                    .build();
            scenario.getConditions().add(scenarioCondition);
        }

        for (DeviceActionAvro actionAvro : scenarioAdded.getActions()) {
            Action action = Action.builder()
                    .type(actionAvro.getType().name())
                    .value(actionAvro.getValue())
                    .build();
            action = actionRepository.save(action);

            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Sensor not found: " + actionAvro.getSensorId()));

            ScenarioActionId id = new ScenarioActionId(scenario.getId(), sensor.getId(), action.getId());
            ScenarioAction scenarioAction = ScenarioAction.builder()
                    .id(id)
                    .scenario(scenario)
                    .sensor(sensor)
                    .action(action)
                    .build();
            scenario.getActions().add(scenarioAction);
        }

        // Сохраните сценарий после добавления всех условий и действий
        scenarioRepository.save(scenario);
    }

    private Integer convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        return Integer.parseInt(value.toString());
    }
}