package ru.yandex.practicum.collector.model.hub;

import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Object value;

    @JsonSetter("value")
    public void setValue(Object value) {
        if (value instanceof Integer || value instanceof Boolean || value == null) {
            this.value = value;
        } else if (value instanceof Number) {
            this.value = ((Number) value).intValue();
        } else {
            throw new IllegalArgumentException("Invalid value type: " + value.getClass() + ". Allowed types: Integer, Boolean, null");
        }
    }
}