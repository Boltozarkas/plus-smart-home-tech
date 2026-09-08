package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryItem;
import ru.yandex.practicum.inventory.exception.ConflictException;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    @Transactional(readOnly = true)
    public List<InventoryDto> getAllInventory() {
        return inventoryRepository.findAll().stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public InventoryDto getInventoryByProductId(Long productId) {
        InventoryItem item = findByProductId(productId);
        return toDto(item);
    }

    @Transactional
    public InventoryDto createInventory(UpdateInventoryRequest request) {
        if (inventoryRepository.existsByProductId(request.productId())) {
            throw new ConflictException(
                    "Складская запись для товара с id " + request.productId() + " уже существует");
        }

        InventoryItem item = new InventoryItem();
        item.setProductId(request.productId());
        item.setQuantity(request.quantity());
        item.setReservedQuantity(0);

        InventoryItem saved = inventoryRepository.save(item);
        log.info("Создана складская запись для товара {}: {}", request.productId(), request.quantity());
        return toDto(saved);
    }

    @Transactional
    public InventoryDto updateInventory(UpdateInventoryRequest request) {
        InventoryItem item = findByProductId(request.productId());

        if (request.quantity() < item.getReservedQuantity()) {
            throw new InsufficientStockException(
                    String.format("Невозможно установить количество %d для товара с id %d. " +
                                    "Зарезервировано: %d. Освободите резерв перед уменьшением.",
                            request.quantity(), request.productId(), item.getReservedQuantity())
            );
        }

        item.setQuantity(request.quantity());

        InventoryItem updated = inventoryRepository.save(item);
        log.info("Обновлено количество товара {}: {}", request.productId(), request.quantity());
        return toDto(updated);
    }

    @Transactional
    public ReserveResponse reserveInventory(ReserveRequest request) {
        InventoryItem item = findByProductId(request.productId());

        int availableQuantity = item.getAvailableQuantity();
        if (availableQuantity < request.quantity()) {
            throw new InsufficientStockException(
                    String.format("Недостаточно товара с id %d. Доступно: %d, запрошено: %d",
                            request.productId(), availableQuantity, request.quantity())
            );
        }

        item.setReservedQuantity(item.getReservedQuantity() + request.quantity());

        InventoryItem updated = inventoryRepository.save(item);
        log.info("Зарезервировано {} единиц товара {}", request.quantity(), request.productId());

        return new ReserveResponse(
                true,
                updated.getAvailableQuantity(),
                "Товар успешно зарезервирован"
        );
    }

    @Transactional
    public ReserveResponse releaseInventory(ReserveRequest request) {
        InventoryItem item = findByProductId(request.productId());

        if (item.getReservedQuantity() < request.quantity()) {
            throw new IllegalArgumentException(
                    String.format("Невозможно снять резерв %d для товара с id %d. " +
                                    "Зарезервировано: %d",
                            request.quantity(), request.productId(), item.getReservedQuantity())
            );
        }

        item.setReservedQuantity(item.getReservedQuantity() - request.quantity());

        InventoryItem updated = inventoryRepository.save(item);
        log.info("Снят резерв {} единиц товара {}", request.quantity(), request.productId());

        return new ReserveResponse(
                true,
                updated.getAvailableQuantity(),
                "Резерв успешно снят"
        );
    }

    private InventoryItem findByProductId(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException(
                        "Складская запись не найдена для товара с id: " + productId));
    }

    private InventoryDto toDto(InventoryItem item) {
        return new InventoryDto(
                item.getId(),
                item.getProductId(),
                item.getQuantity(),
                item.getReservedQuantity(),
                item.getAvailableQuantity()
        );
    }
}