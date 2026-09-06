package ru.yandex.practicum.inventory.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.service.InventoryService;

import java.util.List;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    @GetMapping
    public List<InventoryDto> getAllInventory() {
        return inventoryService.getAllInventory();
    }

    @GetMapping("/{productId}")
    public InventoryDto getInventoryByProductId(@PathVariable Long productId) {
        return inventoryService.getInventoryByProductId(productId);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public InventoryDto createInventory(@Valid @RequestBody UpdateInventoryRequest request) {
        return inventoryService.createInventory(request);
    }

    @PutMapping
    public InventoryDto updateInventory(@Valid @RequestBody UpdateInventoryRequest request) {
        return inventoryService.updateInventory(request);
    }

    @PostMapping("/reserve")
    public ReserveResponse reserveInventory(@Valid @RequestBody ReserveRequest request) {
        return inventoryService.reserveInventory(request);
    }
}