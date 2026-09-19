package ru.yandex.practicum.order.client;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.dto.InventoryRequest;
import ru.yandex.practicum.order.dto.InventoryResponse;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;

@Component
@Slf4j
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {

    @Override
    public InventoryClient create(Throwable cause) {
        return new InventoryClient() {
            @Override
            public InventoryResponse reserve(InventoryRequest request) {
                // Пропускаем бизнес-ошибки дальше
                if (cause instanceof FeignException.NotFound) {
                    throw (FeignException.NotFound) cause;
                }
                if (cause instanceof FeignException.Conflict) {
                    throw (FeignException.Conflict) cause;
                }

                log.warn("Inventory service unavailable for reserve operation. Product id: {}. Cause: {}",
                        request.productId(), cause.getMessage());
                throw new InventoryServiceUnavailableException(
                        request.productId(), "reserve", cause);
            }

            @Override
            public InventoryResponse release(InventoryRequest request) {
                // Пропускаем бизнес-ошибки дальше
                if (cause instanceof FeignException.NotFound) {
                    throw (FeignException.NotFound) cause;
                }
                if (cause instanceof FeignException.BadRequest) {
                    throw (FeignException.BadRequest) cause;
                }

                log.warn("Inventory service unavailable for release operation. Product id: {}. Cause: {}",
                        request.productId(), cause.getMessage());
                throw new InventoryServiceUnavailableException(
                        request.productId(), "release", cause);
            }
        };
    }
}