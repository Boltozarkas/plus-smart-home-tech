package ru.yandex.practicum.order.dto;

import java.math.BigDecimal;

public record ProductDto(
        Long id,
        String name,
        String description,
        BigDecimal price,
        Boolean active
) {
}