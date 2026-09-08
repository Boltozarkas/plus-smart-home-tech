package ru.yandex.practicum.inventory.exception;

public class InvalidReserveReleaseException extends RuntimeException {

    public InvalidReserveReleaseException(String message) {
        super(message);
    }
}