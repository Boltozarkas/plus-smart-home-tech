package ru.yandex.practicum.order.exception;

public class InventoryServiceUnavailableException extends RuntimeException {

    public InventoryServiceUnavailableException(Long productId, String operation, Throwable cause) {
        super("Inventory service unavailable for product id: " + productId + ", operation: " + operation, cause);
    }
}