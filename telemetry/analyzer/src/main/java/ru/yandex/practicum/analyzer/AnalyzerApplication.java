package ru.yandex.practicum.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.analyzer.processor.SnapshotProcessor;

@Slf4j
@SpringBootApplication
public class AnalyzerApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);

        HubEventProcessor hubEventProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");

        // Создаём поток для SnapshotProcessor
        Thread snapshotThread = new Thread(snapshotProcessor::start);
        snapshotThread.setName("SnapshotProcessorThread");

        // Добавляем shutdown hook для graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, waking up consumers...");

            // Вызываем wakeup() из потока завершения приложения
            hubEventProcessor.shutdown();
            snapshotProcessor.shutdown();

            // Ждём завершения потоков
            try {
                hubEventsThread.join(5000);
                snapshotThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while waiting for threads to stop", e);
            }

            log.info("Shutdown hook completed");
        }));

        hubEventsThread.start();
        snapshotThread.start();
    }
}