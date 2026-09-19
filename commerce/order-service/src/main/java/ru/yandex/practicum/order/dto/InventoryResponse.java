package ru.yandex.practicum.order.dto;

public record InventoryResponse(
        boolean success,
        Integer availableQuantity,
        String message
) {
}