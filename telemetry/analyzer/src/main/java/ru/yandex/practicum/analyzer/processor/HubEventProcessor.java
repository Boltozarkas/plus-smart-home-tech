package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, SpecificRecordBase> hubEventConsumer;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final PlatformTransactionManager transactionManager;

    @Value("${kafka.consumer.hub-events.topic}")
    private String hubEventsTopic;

    @Override
    public void run() {
        try {
            hubEventConsumer.subscribe(List.of(hubEventsTopic));
            log.info("HubEventProcessor subscribed to: {}", hubEventsTopic);

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = hubEventConsumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    continue;
                }

                boolean allProcessed = true;

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    try {
                        HubEventAvro event = (HubEventAvro) record.value();
                        processHubEvent(event);
                    } catch (Exception e) {
                        log.error("Failed to process hub event from partition {}, offset {}",
                                record.partition(), record.offset(), e);
                        allProcessed = false;
                        break;
                    }
                }

                if (allProcessed) {
                    hubEventConsumer.commitSync();
                } else {
                    log.warn("Skipping offset commit due to processing errors");
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup exception received in HubEventProcessor, shutting down...");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            hubEventConsumer.close();
            log.info("HubEventProcessor consumer closed");
        }
    }

    public void shutdown() {
        hubEventConsumer.wakeup();
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
            saveScenarioInTransaction(event.getHubId(), scenarioAdded);
            log.info("Scenario added: {}", scenarioAdded.getName());
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            scenarioRepository.findByHubIdAndName(event.getHubId(), scenarioRemoved.getName())
                    .ifPresent(scenarioRepository::delete);
            log.info("Scenario removed: {}", scenarioRemoved.getName());
        }
    }

    private void saveScenarioInTransaction(String hubId, ScenarioAddedEventAvro scenarioAdded) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        transactionTemplate.execute(status -> {
            saveScenario(hubId, scenarioAdded);
            return null;
        });
    }

    private void saveScenario(String hubId, ScenarioAddedEventAvro scenarioAdded) {
        // Ищем существующий сценарий
        Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(hubId, scenarioAdded.getName());

        Scenario scenario;
        if (existingScenario.isPresent()) {
            // Обновляем существующий сценарий
            scenario = existingScenario.get();
            log.info("Updating existing scenario: hubId={}, name={}", hubId, scenarioAdded.getName());

            // Очищаем коллекции
            scenario.getConditions().clear();
            scenario.getActions().clear();
            scenarioRepository.save(scenario);
        } else {
            // Создаём новый сценарий
            scenario = Scenario.builder()
                    .hubId(hubId)
                    .name(scenarioAdded.getName())
                    .build();
            scenario = scenarioRepository.save(scenario);
            log.info("Creating new scenario: hubId={}, name={}", hubId, scenarioAdded.getName());
        }

        // Добавляем условия
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

        // Добавляем действия
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

        // Сохраняем сценарий
        scenario = scenarioRepository.save(scenario);
        log.info("Scenario saved: hubId={}, name={}, conditions={}, actions={}",
                hubId, scenarioAdded.getName(),
                scenario.getConditions().size(),
                scenario.getActions().size());
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