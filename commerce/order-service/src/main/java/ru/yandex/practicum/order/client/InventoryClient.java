package ru.yandex.practicum.order.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.order.dto.InventoryRequest;
import ru.yandex.practicum.order.dto.InventoryResponse;

@FeignClient(
        name = "inventory-service",
        fallbackFactory = InventoryClientFallbackFactory.class
)
public interface InventoryClient {

    @PostMapping("/api/inventory/reserve")
    InventoryResponse reserve(@RequestBody InventoryRequest request);

    @PostMapping("/api/inventory/release")
    InventoryResponse release(@RequestBody InventoryRequest request);
}