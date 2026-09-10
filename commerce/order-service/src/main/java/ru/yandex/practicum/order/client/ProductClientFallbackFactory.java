package ru.yandex.practicum.order.client;

import feign.FeignException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.dto.ProductDto;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;

@Component
@Slf4j
public class ProductClientFallbackFactory implements FallbackFactory<ProductClient> {

    @Override
    public ProductClient create(Throwable cause) {
        return new ProductClient() {
            @Override
            public ProductDto getProductById(Long id) {
                // Пропускаем бизнес-ошибки дальше (404 - товар не найден)
                if (cause instanceof FeignException.NotFound) {
                    throw (FeignException.NotFound) cause;
                }
                if (cause instanceof FeignException.Conflict) {
                    throw (FeignException.Conflict) cause;
                }

                log.warn("Product service unavailable for product id: {}. Cause: {}",
                        id, cause.getMessage());
                throw new ProductServiceUnavailableException(id, cause);
            }
        };
    }
}