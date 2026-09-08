package ru.yandex.practicum.order.service;

import feign.FeignException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.dto.*;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private ProductClient productClient;

    @Mock
    private InventoryClient inventoryClient;

    @InjectMocks
    private OrderService orderService;

    private ProductDto activeProduct;
    private ProductDto inactiveProduct;
    private InventoryResponse successReserveResponse;
    private InventoryResponse successReleaseResponse;

    @BeforeEach
    void setUp() {
        activeProduct = new ProductDto(
                1L,
                "SHT LED Smart Bulb W3",
                "Умная лампа",
                new BigDecimal("1490.00"),
                true
        );

        inactiveProduct = new ProductDto(
                2L,
                "Старый товар",
                "Снят с продажи",
                new BigDecimal("100.00"),
                false
        );

        successReserveResponse = new InventoryResponse(true, 95, "Товар успешно зарезервирован");
        successReleaseResponse = new InventoryResponse(true, 100, "Резерв успешно снят");
    }

    @Test
    @DisplayName("Успешное создание заказа")
    void createOrder_Success() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(new OrderItemRequest(1L, 2))
        );

        when(productClient.getProductById(1L)).thenReturn(activeProduct);
        when(inventoryClient.reserve(any(InventoryRequest.class))).thenReturn(successReserveResponse);
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            order.setId(1L);
            return order;
        });

        // When
        OrderDto result = orderService.createOrder(request);

        // Then
        assertNotNull(result);
        assertEquals("CONFIRMED", result.status());
        assertEquals(new BigDecimal("2980.00"), result.totalPrice());
        assertEquals(1, result.items().size());

        // Verify
        verify(productClient, times(1)).getProductById(1L);
        verify(inventoryClient, times(1)).reserve(any(InventoryRequest.class));
        verify(orderRepository, times(1)).save(any(Order.class));
        verify(inventoryClient, never()).release(any(InventoryRequest.class));
    }

    @Test
    @DisplayName("Повторяющийся productId - товар запрашивается один раз, резерв суммарный")
    void createOrder_DuplicateProductId() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(
                        new OrderItemRequest(1L, 2),
                        new OrderItemRequest(1L, 3)
                )
        );

        when(productClient.getProductById(1L)).thenReturn(activeProduct);
        when(inventoryClient.reserve(any(InventoryRequest.class))).thenReturn(successReserveResponse);
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            order.setId(1L);
            return order;
        });

        // When
        OrderDto result = orderService.createOrder(request);

        // Then
        assertNotNull(result);
        assertEquals("CONFIRMED", result.status());

        // Проверяем, что товар запрошен один раз
        verify(productClient, times(1)).getProductById(1L);

        // Проверяем, что резерв создан с суммарным количеством
        ArgumentCaptor<InventoryRequest> captor = ArgumentCaptor.forClass(InventoryRequest.class);
        verify(inventoryClient, times(1)).reserve(captor.capture());
        assertEquals(5, captor.getValue().quantity());
        assertEquals(1L, captor.getValue().productId());

        verify(orderRepository, times(1)).save(any(Order.class));
    }

    @Test
    @DisplayName("Товар снят с продажи - резерв не выполняется, заказ не сохраняется")
    void createOrder_InactiveProduct() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(new OrderItemRequest(2L, 1))
        );

        when(productClient.getProductById(2L)).thenReturn(inactiveProduct);

        // When & Then
        assertThrows(OrderProcessingException.class, () -> orderService.createOrder(request));

        // Verify
        verify(productClient, times(1)).getProductById(2L);
        verify(inventoryClient, never()).reserve(any(InventoryRequest.class));
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    @DisplayName("Ошибка резервирования - заказ не сохраняется")
    void createOrder_ReserveError() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(new OrderItemRequest(1L, 1000))
        );

        when(productClient.getProductById(1L)).thenReturn(activeProduct);

        // Мокаем ошибку резервирования
        FeignException.Conflict conflictException = mock(FeignException.Conflict.class);
        when(inventoryClient.reserve(any(InventoryRequest.class)))
                .thenThrow(conflictException);

        // When & Then
        assertThrows(OrderProcessingException.class, () -> orderService.createOrder(request));

        // Verify
        verify(productClient, times(1)).getProductById(1L);
        verify(inventoryClient, times(1)).reserve(any(InventoryRequest.class));
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    @DisplayName("Ошибка после успешного резерва - резерв снимается")
    void createOrder_ErrorAfterReserve() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(
                        new OrderItemRequest(1L, 2),  // Первый товар успешно резервируется
                        new OrderItemRequest(2L, 1)   // Второй товар - ошибка
                )
        );

        // Первый товар успешно резервируется
        when(productClient.getProductById(1L)).thenReturn(activeProduct);
        when(inventoryClient.reserve(any(InventoryRequest.class)))
                .thenReturn(successReserveResponse);

        // Второй товар снят с продажи
        when(productClient.getProductById(2L)).thenReturn(inactiveProduct);

        // Мокаем release
        when(inventoryClient.release(any(InventoryRequest.class)))
                .thenReturn(successReleaseResponse);

        // When & Then
        assertThrows(OrderProcessingException.class, () -> orderService.createOrder(request));

        // Verify - резерв для первого товара должен быть снят
        verify(inventoryClient, times(1)).reserve(any(InventoryRequest.class));
        verify(inventoryClient, times(1)).release(any(InventoryRequest.class));
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    @DisplayName("Ошибка сохранения заказа - резерв снимается")
    void createOrder_SaveError() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(new OrderItemRequest(1L, 2))
        );

        when(productClient.getProductById(1L)).thenReturn(activeProduct);
        when(inventoryClient.reserve(any(InventoryRequest.class))).thenReturn(successReserveResponse);
        when(inventoryClient.release(any(InventoryRequest.class))).thenReturn(successReleaseResponse);

        // Мокаем ошибку сохранения
        when(orderRepository.save(any(Order.class)))
                .thenThrow(new RuntimeException("Ошибка сохранения"));

        // When & Then
        assertThrows(OrderProcessingException.class, () -> orderService.createOrder(request));

        // Verify - резерв должен быть снят
        verify(inventoryClient, times(1)).reserve(any(InventoryRequest.class));
        verify(inventoryClient, times(1)).release(any(InventoryRequest.class));
    }

    @Test
    @DisplayName("Множественные резервы - при ошибке все снимаются")
    void createOrder_MultipleReservesWithError() {
        // Given
        CreateOrderRequest request = new CreateOrderRequest(
                "Иван Петров",
                "ivan@example.com",
                List.of(
                        new OrderItemRequest(1L, 2),  // Первый товар
                        new OrderItemRequest(2L, 1)   // Второй товар
                )
        );

        // Первый товар успешно резервируется
        when(productClient.getProductById(1L)).thenReturn(activeProduct);
        when(productClient.getProductById(2L)).thenReturn(inactiveProduct);
        when(inventoryClient.reserve(any(InventoryRequest.class)))
                .thenReturn(successReserveResponse);
        when(inventoryClient.release(any(InventoryRequest.class)))
                .thenReturn(successReleaseResponse);

        // When & Then
        assertThrows(OrderProcessingException.class, () -> orderService.createOrder(request));

        // Verify
        verify(inventoryClient, times(1)).reserve(any(InventoryRequest.class));
        verify(inventoryClient, times(1)).release(any(InventoryRequest.class));
        verify(orderRepository, never()).save(any(Order.class));
    }
}