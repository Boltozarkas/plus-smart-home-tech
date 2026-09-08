package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.dto.*;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final ProductClient productClient;
    private final InventoryClient inventoryClient;

    public OrderDto createOrder(CreateOrderRequest request) {
        log.info("Начало создания заказа для {}", request.customerEmail());

        // Список для хранения успешных резервов
        List<InventoryRequest> successfulReserves = new ArrayList<>();

        try {
            // Группируем позиции по productId и суммируем количество
            Map<Long, Integer> groupedItems = request.items().stream()
                    .collect(Collectors.groupingBy(
                            OrderItemRequest::productId,
                            Collectors.summingInt(OrderItemRequest::quantity)
                    ));

            log.info("Сгруппированные позиции: {}", groupedItems);

            // Получаем данные о товарах и резервируем
            List<OrderItemData> itemDataList = new ArrayList<>();

            for (Map.Entry<Long, Integer> entry : groupedItems.entrySet()) {
                Long productId = entry.getKey();
                Integer quantity = entry.getValue();

                // Получаем данные о товаре из product-service
                ProductDto product = getProduct(productId);

                // Проверяем, что товар активен
                if (!product.active()) {
                    throw new OrderProcessingException(
                            "Товар с id " + productId + " снят с продажи");
                }

                // Резервируем товар
                InventoryRequest reserveRequest = new InventoryRequest(productId, quantity);
                InventoryResponse reserveResponse = reserveInventory(reserveRequest);
                successfulReserves.add(reserveRequest);

                // Сохраняем данные о товаре
                itemDataList.add(new OrderItemData(
                        productId,
                        product.name(),
                        quantity,
                        product.price()
                ));

                log.info("Зарезервировано {} единиц товара {}", quantity, productId);
            }

            // Создаем заказ
            Order order = new Order();
            order.setCustomerName(request.customerName());
            order.setCustomerEmail(request.customerEmail());
            order.setStatus("CONFIRMED");
            order.setStatusDetails("Заказ подтвержден");

            // Добавляем позиции
            for (OrderItemData itemData : itemDataList) {
                OrderItem item = new OrderItem();
                item.setProductId(itemData.productId());
                item.setProductName(itemData.productName());
                item.setQuantity(itemData.quantity());
                item.setPrice(itemData.price());
                order.addItem(item);
            }

            // Рассчитываем общую стоимость
            order.calculateTotalPrice();

            // Сохраняем заказ
            Order saved = orderRepository.save(order);
            log.info("Заказ создан с id={}, статус={}", saved.getId(), saved.getStatus());

            return toDto(saved);

        } catch (OrderProcessingException e) {
            // Компенсация: снимаем резервы
            releaseReserves(successfulReserves);
            throw e;
        } catch (Exception e) {
            // Компенсация: снимаем резервы
            releaseReserves(successfulReserves);
            log.error("Ошибка при создании заказа", e);
            throw new OrderProcessingException("Ошибка обработки заказа: " + e.getMessage());
        }
    }

    private ProductDto getProduct(Long productId) {
        try {
            return productClient.getProductById(productId);
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Товар с id " + productId + " не найден");
        } catch (FeignException e) {
            throw new OrderProcessingException("Ошибка при получении товара с id " + productId);
        }
    }

    private InventoryResponse reserveInventory(InventoryRequest request) {
        try {
            return inventoryClient.reserve(request);
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException(
                    "Складская запись не найдена для товара с id " + request.productId());
        } catch (FeignException.Conflict e) {
            throw new OrderProcessingException(
                    "Недостаточно товара с id " + request.productId());
        } catch (FeignException e) {
            throw new OrderProcessingException(
                    "Ошибка при резервировании товара с id " + request.productId());
        }
    }

    private void releaseReserves(List<InventoryRequest> reserves) {
        for (InventoryRequest reserve : reserves) {
            try {
                inventoryClient.release(reserve);
                log.info("Снят резерв для товара {} в количестве {}",
                        reserve.productId(), reserve.quantity());
            } catch (Exception e) {
                log.error("Ошибка при снятии резерва для товара {}", reserve.productId(), e);
            }
        }
    }

    @Transactional(readOnly = true)
    public OrderDto getOrderById(Long id) {
        Order order = findOrderById(id);
        return toDto(order);
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getAllOrders() {
        return orderRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getOrdersByEmail(String email) {
        return orderRepository.findByCustomerEmailIgnoreCase(email).stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    private Order findOrderById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ не найден с id: " + id));
    }

    private OrderDto toDto(Order order) {
        List<OrderItemDto> itemDtos = order.getItems().stream()
                .map(this::toItemDto)
                .collect(Collectors.toList());

        return new OrderDto(
                order.getId(),
                order.getCustomerName(),
                order.getCustomerEmail(),
                order.getStatus(),
                order.getTotalPrice(),
                order.getStatusDetails(),
                order.getCreatedAt(),
                itemDtos
        );
    }

    private OrderItemDto toItemDto(OrderItem item) {
        return new OrderItemDto(
                item.getId(),
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
        );
    }

    // Вспомогательный класс для хранения данных о товаре
    private record OrderItemData(
            Long productId,
            String productName,
            Integer quantity,
            BigDecimal price
    ) {
    }
}