package ru.yandex.practicum.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Aggregator {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Aggregator.class, args);
        AggregationStarter aggregator = context.getBean(AggregationStarter.class);

        // Добавляем shutdown hook для корректной остановки
        Runtime.getRuntime().addShutdownHook(new Thread(aggregator::shutdown));

        aggregator.start();
    }
}