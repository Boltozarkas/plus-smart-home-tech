package ru.yandex.practicum.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEvent event) {
        Object payload = null;

        if (event instanceof DeviceAddedEvent e) {
            payload = DeviceAddedEventAvro.newBuilder()
                    .setId(e.getId())
                    .setType(mapDeviceType(e.getDeviceType()))
                    .build();
        } else if (event instanceof DeviceRemovedEvent e) {
            payload = DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getId())
                    .build();
        } else if (event instanceof ScenarioAddedEvent e) {
            payload = ScenarioAddedEventAvro.newBuilder()
                    .setName(e.getName())
                    .setConditions(mapConditions(e.getConditions()))
                    .setActions(mapActions(e.getActions()))
                    .build();
        } else if (event instanceof ScenarioRemovedEvent e) {
            payload = ScenarioRemovedEventAvro.newBuilder()
                    .setName(e.getName())
                    .build();
        } else {
            throw new IllegalArgumentException("Unknown hub event type: " + event.getClass().getSimpleName());
        }

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private DeviceTypeAvro mapDeviceType(DeviceType deviceType) {
        return DeviceTypeAvro.valueOf(deviceType.name());
    }

    private List<ScenarioConditionAvro> mapConditions(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(condition -> {
                    ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                            .setSensorId(condition.getSensorId())
                            .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                            .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

                    Object value = condition.getValue();
                    if (value instanceof Integer) {
                        builder.setValue((Integer) value);
                    } else if (value instanceof Boolean) {
                        builder.setValue((Boolean) value);
                    }

                    return builder.build();
                })
                .collect(Collectors.toList());
    }

    private List<DeviceActionAvro> mapActions(List<DeviceAction> actions) {
        return actions.stream()
                .map(action -> DeviceActionAvro.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(ActionTypeAvro.valueOf(action.getType().name()))
                        .setValue(action.getValue())
                        .build())
                .collect(Collectors.toList());
    }
}