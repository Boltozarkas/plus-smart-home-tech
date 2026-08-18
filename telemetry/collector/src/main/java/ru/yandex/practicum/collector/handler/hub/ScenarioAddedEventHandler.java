package ru.yandex.practicum.collector.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.handler.HubEventHandler;
import ru.yandex.practicum.collector.service.KafkaEventSender;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final KafkaEventSender kafkaEventSender;

    public ScenarioAddedEventHandler(KafkaEventSender kafkaEventSender) {
        this.kafkaEventSender = kafkaEventSender;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        ScenarioAddedEventProto scenarioAdded = event.getScenarioAdded();
        ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAdded.getName())
                .setConditions(mapConditions(scenarioAdded.getConditionList()))
                .setActions(mapActions(scenarioAdded.getActionList()))
                .build();

        HubEventAvro avroEvent = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(payload)
                .build();

        kafkaEventSender.sendHubEvent(event.getHubId(), avroEvent, avroEvent.getTimestamp());
    }

    private List<ScenarioConditionAvro> mapConditions(List<ScenarioConditionProto> conditions) {
        return conditions.stream()
                .map(condition -> {
                    ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                            .setSensorId(condition.getSensorId())
                            .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                            .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

                    switch (condition.getValueCase()) {
                        case INT_VALUE -> builder.setValue(condition.getIntValue());
                        case BOOL_VALUE -> builder.setValue(condition.getBoolValue());
                        default -> { /* value is null */ }
                    }

                    return builder.build();
                })
                .collect(Collectors.toList());
    }

    private List<DeviceActionAvro> mapActions(List<DeviceActionProto> actions) {
        return actions.stream()
                .map(action -> DeviceActionAvro.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(ActionTypeAvro.valueOf(action.getType().name()))
                        .setValue(action.hasValue() ? action.getValue() : null)
                        .build())
                .collect(Collectors.toList());
    }
}