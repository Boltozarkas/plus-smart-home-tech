package ru.yandex.practicum.product.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateProductRequest(

        @NotBlank(message = "Название товара обязательно")
        @Size(max = 255, message = "Название не может быть длиннее 255 символов")
        String name,

        @Size(max = 2000, message = "Описание не может быть длиннее 2000 символов")
        String description,

        @NotNull(message = "Цена обязательна")
        @DecimalMin(value = "0.01", message = "Цена должна быть больше нуля")
        BigDecimal price,

        Long categoryId,

        String imageUrl
) {
}