package ru.yandex.practicum.gateway.security;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.springframework.web.reactive.function.server.RequestPredicates.path;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureWebTestClient
class GatewaySecurityConfigTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void catalogGet_isPublic() {
        webTestClient.get()
                .uri("/api/products")
                .exchange()
                .expectStatus().value(status -> {
                    org.junit.jupiter.api.Assertions.assertNotEquals(401, status);
                    org.junit.jupiter.api.Assertions.assertNotEquals(403, status);
                });
    }

    @Test
    void orderCreate_withoutCredentials_isUnauthorized() {
        webTestClient.post()
                .uri("/api/orders")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void orderCreate_withUserCredentials_passesSecurity() {
        webTestClient.post()
                .uri("/api/orders")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().value(status -> {
                    org.junit.jupiter.api.Assertions.assertNotEquals(401, status);
                    org.junit.jupiter.api.Assertions.assertNotEquals(403, status);
                });
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
    void productWrite_withAdminCredentials_passesSecurity() {
        webTestClient.post()
                .uri("/api/products")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().value(status -> {
                    org.junit.jupiter.api.Assertions.assertNotEquals(401, status);
                    org.junit.jupiter.api.Assertions.assertNotEquals(403, status);
                });
    }

    @Test
    void orderList_withUserCredentials_isForbidden() {
        webTestClient.get()
                .uri("/api/orders")
                .header("Authorization", basic("ivan", "ivan"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void orderList_withAdminCredentials_passesSecurity() {
        webTestClient.get()
                .uri("/api/orders")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().value(status -> {
                    org.junit.jupiter.api.Assertions.assertNotEquals(401, status);
                    org.junit.jupiter.api.Assertions.assertNotEquals(403, status);
                });
    }

    @Test
    void unknownRoute_withAdminCredentials_isForbidden() {
        webTestClient.get()
                .uri("/api/unknown")
                .header("Authorization", basic("anna", "anna"))
                .exchange()
                .expectStatus().isForbidden();
    }

    @Test
    void corsPreflight_isPublic() {
        webTestClient.options()
                .uri("/api/orders")
                .header("Origin", "http://localhost:8443")
                .header("Access-Control-Request-Method", "POST")
                .exchange()
                .expectStatus().value(status -> {
                    org.junit.jupiter.api.Assertions.assertNotEquals(401, status);
                    org.junit.jupiter.api.Assertions.assertNotEquals(403, status);
                });
    }

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