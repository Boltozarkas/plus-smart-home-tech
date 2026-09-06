package ru.yandex.practicum.product.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.product.entity.Category;

@Repository
public interface CategoryRepository extends JpaRepository<Category, Long> {
}