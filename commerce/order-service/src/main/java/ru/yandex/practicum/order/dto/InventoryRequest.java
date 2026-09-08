package ru.yandex.practicum.order.dto;

public record InventoryRequest(
        Long productId,
        Integer quantity
) {
}