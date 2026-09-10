package ru.yandex.practicum.order.service;

public record ServiceCallResult<T>(
        T data,
        boolean degraded,
        String degradedReason
) {
    public static <T> ServiceCallResult<T> success(T data) {
        return new ServiceCallResult<>(data, false, null);
    }

    public static <T> ServiceCallResult<T> degraded(String reason) {
        return new ServiceCallResult<>(null, true, reason);
    }

    public boolean isSuccess() {
        return !degraded;
    }

    public boolean isDegraded() {
        return degraded;
    }
}