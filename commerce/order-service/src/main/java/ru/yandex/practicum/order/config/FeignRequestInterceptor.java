package ru.yandex.practicum.order.config;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.UUID;

@Component
@Slf4j
public class FeignRequestInterceptor implements RequestInterceptor {

    private static final String X_REQUEST_ID = "X-Request-Id";
    private static final String X_SOURCE_SERVICE = "X-Source-Service";
    private static final String SOURCE_SERVICE_NAME = "order-service";

    @Override
    public void apply(RequestTemplate template) {
        // Передаем источник вызова
        template.header(X_SOURCE_SERVICE, SOURCE_SERVICE_NAME);

        // Передаем X-Request-Id
        String requestId = getRequestId();
        template.header(X_REQUEST_ID, requestId);

        log.debug("Feign запрос: {} {}, X-Request-Id: {}",
                template.method(), template.url(), requestId);
    }

    private String getRequestId() {
        // Пытаемся получить текущий HTTP-запрос
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();

        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            String requestId = request.getHeader(X_REQUEST_ID);
            if (requestId != null && !requestId.isEmpty()) {
                return requestId;
            }
        }

        // Если запроса нет или нет заголовка, создаем новый ID
        return UUID.randomUUID().toString();
    }
}