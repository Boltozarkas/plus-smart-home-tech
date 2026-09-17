package ru.yandex.practicum.gateway.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.server.SecurityWebFilterChain;

import java.util.List;

@Configuration
@EnableWebFluxSecurity
public class GatewaySecurityConfig {

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .cors(cors -> {})  // используем CORS из gateway конфига
                .authorizeExchange(exchanges -> exchanges
                        // Preflight OPTIONS - публичный
                        .pathMatchers(HttpMethod.OPTIONS, "/**").permitAll()

                        // Swagger / OpenAPI
                        .pathMatchers(
                                "/v3/api-docs/**",
                                "/swagger-ui/**",
                                "/swagger-ui.html",
                                "/webjars/**",
                                "/swagger-resources/**"
                        ).permitAll()

                        // Публичные GET-маршруты
                        .pathMatchers(HttpMethod.GET, "/api/products/**").permitAll()
                        .pathMatchers(HttpMethod.GET, "/api/categories/**").permitAll()
                        .pathMatchers(HttpMethod.GET, "/api/inventory/**").permitAll()

                        // Пользовательские маршруты (USER или ADMIN)
                        .pathMatchers(HttpMethod.POST, "/api/orders/**").hasAnyRole("USER", "ADMIN")
                        .pathMatchers(HttpMethod.GET, "/api/orders/by-email").hasAnyRole("USER", "ADMIN")
                        .pathMatchers(HttpMethod.GET, "/api/orders/{id}").hasAnyRole("USER", "ADMIN")

                        // Административные маршруты
                        .pathMatchers(HttpMethod.GET, "/api/orders").hasRole("ADMIN")

                        // Изменяющие маршруты каталога и склада
                        .pathMatchers(HttpMethod.POST, "/api/products/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PATCH, "/api/products/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PUT, "/api/products/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.DELETE, "/api/products/**").hasRole("ADMIN")

                        .pathMatchers(HttpMethod.POST, "/api/categories/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PATCH, "/api/categories/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PUT, "/api/categories/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.DELETE, "/api/categories/**").hasRole("ADMIN")

                        .pathMatchers(HttpMethod.POST, "/api/inventory/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PUT, "/api/inventory/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.PATCH, "/api/inventory/**").hasRole("ADMIN")
                        .pathMatchers(HttpMethod.DELETE, "/api/inventory/**").hasRole("ADMIN")

                        // Всё остальное — запрещено
                        .anyExchange().denyAll()
                )
                .httpBasic(httpBasic -> {})
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public MapReactiveUserDetailsService userDetailsService(
            AppSecurityProperties properties,
            PasswordEncoder passwordEncoder) {

        List<UserDetails> users = properties.getUsers().stream()
                .map(u -> User.builder()
                        .username(u.getUsername())
                        .password(passwordEncoder.encode(u.getPassword()))
                        .roles(u.getRoles().toArray(new String[0]))
                        .build())
                .toList();

        return new MapReactiveUserDetailsService(users);
    }
}