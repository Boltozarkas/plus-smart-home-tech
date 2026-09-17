package ru.yandex.practicum.gateway.security;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "app.security")
@Getter
@Setter
public class AppSecurityProperties {

    private List<User> users = new ArrayList<>();

    @Getter
    @Setter
    public static class User {
        private String username;
        private String password;
        private List<String> roles = new ArrayList<>();
    }
}