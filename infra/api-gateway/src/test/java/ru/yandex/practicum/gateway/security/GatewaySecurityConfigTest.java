package ru.yandex.practicum.gateway.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.springframework.web.reactive.function.server.RequestPredicates.path;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewaySecurityConfigTest {

    @LocalServerPort
    private int port;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        // Явно задаём baseUrl с http://, чтобы схема была не null
        webTestClient = WebTestClient
                .bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    // ===== Публичные маршруты =====

    @Test
    void catalogGet_isPublic() {
        webTestClient.get()
                .uri("/api/products")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void catalogGetById_isPublic() {
        webTestClient.get()
                .uri("/api/products/1")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void categoriesGet_isPublic() {
        webTestClient.get()
                .uri("/api/categories")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void inventoryGet_isPublic() {
        webTestClient.get()
                .uri("/api/inventory")
                .exchange()
                .expectStatus().isOk();
    }

    // ===== Неавторизованные запросы =====

    @Test
    void orderCreate_withoutCredentials_isUnauthorized() {
        webTestClient.post()
                .uri("/api/orders")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void orderList_withoutCredentials_isUnauthorized() {
        webTestClient.get()
                .uri("/api/orders")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void productWrite_withoutCredentials_isUnauthorized() {
        webTestClient.post()
                .uri("/api/products")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    // ===== Пользовательские маршруты =====

    @Test
    void orderCreate_withUserCredentials_isOk() {
        webTestClient.post()
                .uri("/api/orders")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void orderCreate_withAdminCredentials_isOk() {
        webTestClient.post()
                .uri("/api/orders")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    // ===== Административные маршруты =====

    @Test
    void orderList_withUserCredentials_isForbidden() {
        webTestClient.get()
                .uri("/api/orders")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void orderList_withAdminCredentials_isOk() {
        webTestClient.get()
                .uri("/api/orders")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void productWrite_withUserCredentials_isForbidden() {
        webTestClient.post()
                .uri("/api/products")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void productWrite_withAdminCredentials_isOk() {
        webTestClient.post()
                .uri("/api/products")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void categoryWrite_withUserCredentials_isForbidden() {
        webTestClient.post()
                .uri("/api/categories")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void categoryWrite_withAdminCredentials_isOk() {
        webTestClient.post()
                .uri("/api/categories")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    void inventoryWrite_withUserCredentials_isForbidden() {
        webTestClient.post()
                .uri("/api/inventory")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void inventoryWrite_withAdminCredentials_isOk() {
        webTestClient.post()
                .uri("/api/inventory")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isOk();
    }

    // ===== Закрытые маршруты =====

    @Test
    void unknownRoute_withAdminCredentials_isForbidden() {
        webTestClient.get()
                .uri("/api/unknown")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void unknownRoute_withoutCredentials_isUnauthorized() {
        webTestClient.get()
                .uri("/api/unknown")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    // ===== CORS preflight =====

    @Test
    void corsPreflight_returnsCorsHeaders() {
        webTestClient.options()
                .uri("/api/orders")
                .header("Origin", "http://localhost:8443")
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "Authorization, Content-Type")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("Access-Control-Allow-Origin", "http://localhost:8443")
                .expectHeader().valueMatches("Access-Control-Allow-Methods", ".*POST.*")
                .expectHeader().valueEquals("Access-Control-Allow-Credentials", "true");
    }

    @Test
    void corsPreflight_fromUnknownOrigin_doesNotReturnAllowOrigin() {
        webTestClient.options()
                .uri("/api/orders")
                .header("Origin", "http://evil.example.com")
                .header("Access-Control-Request-Method", "POST")
                .exchange()
                .expectStatus().is4xxClientError()
                .expectHeader().doesNotExist("Access-Control-Allow-Origin");
    }

    // ===== Вспомогательные методы =====

    private String basic(String username, String password) {
        String value = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    @TestConfiguration
    static class TestBackendConfig {

        @Bean
        RouterFunction<ServerResponse> testBackendRoutes() {
            return route(path("/api/**"), request -> ServerResponse.ok().build());
        }
    }
}