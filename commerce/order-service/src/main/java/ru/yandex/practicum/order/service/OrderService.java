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
import ru.yandex.practicum.order.exception.*;
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

        Map<Long, Integer> groupedItems = request.items().stream()
                .collect(Collectors.groupingBy(
                        OrderItemRequest::productId,
                        Collectors.summingInt(OrderItemRequest::quantity)
                ));

        log.info("Сгруппированные позиции: {}", groupedItems);

        List<InventoryRequest> successfulReserves = new ArrayList<>();
        boolean degraded = false;
        StringBuilder degradedReasons = new StringBuilder();
        List<OrderItemData> itemDataList = new ArrayList<>();

        try {
            for (Map.Entry<Long, Integer> entry : groupedItems.entrySet()) {
                Long productId = entry.getKey();
                Integer quantity = entry.getValue();

                try {
                    ServiceCallResult<ProductDto> productResult = getProduct(productId);

                    if (productResult.isDegraded()) {
                        degraded = true;
                        degradedReasons.append("Product service unavailable for product ")
                                .append(productId).append("; ");

                        itemDataList.add(new OrderItemData(
                                productId,
                                "Товар #" + productId + " (ожидает проверки)",
                                quantity,
                                BigDecimal.ZERO
                        ));
                        continue;
                    }

                    ProductDto product = productResult.data();

                    if (!product.active()) {
                        throw new OrderProcessingException(
                                "Товар с id " + productId + " снят с продажи");
                    }

                    ServiceCallResult<InventoryResponse> reserveResult = reserveInventory(
                            new InventoryRequest(productId, quantity)
                    );

                    if (reserveResult.isDegraded()) {
                        degraded = true;
                        degradedReasons.append("Inventory service unavailable for product ")
                                .append(productId).append("; ");

                        itemDataList.add(new OrderItemData(
                                productId,
                                product.name(),
                                quantity,
                                product.price()
                        ));
                        continue;
                    }

                    successfulReserves.add(new InventoryRequest(productId, quantity));
                    itemDataList.add(new OrderItemData(
                            productId,
                            product.name(),
                            quantity,
                            product.price()
                    ));

                    log.info("Зарезервировано {} единиц товара {}", quantity, productId);

                } catch (ProductServiceUnavailableException e) {
                    degraded = true;
                    degradedReasons.append("Product service unavailable for product ")
                            .append(productId).append("; ");

                    itemDataList.add(new OrderItemData(
                            productId,
                            "Товар #" + productId + " (ожидает проверки)",
                            quantity,
                            BigDecimal.ZERO
                    ));
                } catch (InventoryServiceUnavailableException e) {
                    degraded = true;
                    degradedReasons.append("Inventory service unavailable for product ")
                            .append(productId).append("; ");

                    String productName = "Товар #" + productId + " (ожидает проверки)";
                    BigDecimal price = BigDecimal.ZERO;
                    try {
                        ProductDto product = productClient.getProductById(productId);
                        productName = product.name();
                        price = product.price();
                    } catch (Exception ex) {
                        log.warn("Не удалось получить данные товара {} при деградации склада: {}",
                                productId, ex.getMessage());
                    }

                    itemDataList.add(new OrderItemData(
                            productId,
                            productName,
                            quantity,
                            price
                    ));
                } catch (OrderProcessingException e) {
                    // Бизнес-ошибка - компенсируем резервы и выбрасываем
                    releaseReserves(successfulReserves);
                    throw e;
                }
            }

            // Создаем заказ
            Order order = new Order();
            order.setCustomerName(request.customerName());
            order.setCustomerEmail(request.customerEmail());

            if (degraded) {
                order.setStatus("PENDING_CONFIRMATION");
                order.setStatusDetails("Заказ требует ручной проверки. Причины: " + degradedReasons);
            } else {
                order.setStatus("CONFIRMED");
                order.setStatusDetails("Заказ подтвержден");
            }

            for (OrderItemData itemData : itemDataList) {
                OrderItem item = new OrderItem();
                item.setProductId(itemData.productId());
                item.setProductName(itemData.productName());
                item.setQuantity(itemData.quantity());
                item.setPrice(itemData.price());
                order.addItem(item);
            }

            order.calculateTotalPrice();

            // ← Сохранение в try-catch для компенсации при ошибке
            Order saved;
            try {
                saved = orderRepository.save(order);
            } catch (Exception e) {
                releaseReserves(successfulReserves);
                log.error("Ошибка сохранения заказа", e);
                throw new OrderProcessingException("Ошибка сохранения заказа: " + e.getMessage());
            }

            log.info("Заказ создан с id={}, статус={}", saved.getId(), saved.getStatus());
            return toDto(saved);

        } catch (OrderProcessingException e) {
            // Уже обработано выше, резервы сняты
            throw e;
        } catch (Exception e) {
            releaseReserves(successfulReserves);
            log.error("Непредвиденная ошибка при создании заказа", e);
            throw new OrderProcessingException("Ошибка обработки заказа: " + e.getMessage());
        }
    }

    private ServiceCallResult<ProductDto> getProduct(Long productId) {
        try {
            ProductDto product = productClient.getProductById(productId);
            return ServiceCallResult.success(product);
        } catch (ProductServiceUnavailableException e) {
            return ServiceCallResult.degraded(e.getMessage());
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Товар с id " + productId + " не найден");
        } catch (FeignException e) {
            throw new OrderProcessingException("Ошибка при получении товара с id " + productId);
        }
    }

    private ServiceCallResult<InventoryResponse> reserveInventory(InventoryRequest request) {
        try {
            InventoryResponse response = inventoryClient.reserve(request);
            return ServiceCallResult.success(response);
        } catch (InventoryServiceUnavailableException e) {
            return ServiceCallResult.degraded(e.getMessage());
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

    private record OrderItemData(
            Long productId,
            String productName,
            Integer quantity,
            BigDecimal price
    ) {
    }
}