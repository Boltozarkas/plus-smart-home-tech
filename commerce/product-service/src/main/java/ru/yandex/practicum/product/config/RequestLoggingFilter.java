package ru.yandex.practicum.product.config;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RequestLoggingFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String requestId = httpRequest.getHeader("X-Request-Id");
        String sourceService = httpRequest.getHeader("X-Source-Service");

        if (requestId != null) {
            log.debug("Получен запрос с X-Request-Id: {}, от сервиса: {}",
                    requestId, sourceService);
        }

        try {
            chain.doFilter(request, response);
        } catch (Exception e) {
            log.error("Ошибка при обработке запроса", e);
        }
    }
}