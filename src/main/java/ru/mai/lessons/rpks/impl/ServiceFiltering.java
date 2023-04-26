package ru.mai.lessons.rpks.impl;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        // Считываем параметры конфигурации для подключения к базе данных
        Config db = config.getConfig("db");

        // Считываем параметры конфигурации для подключения к Kafka
        String consumerServers = config.getString("kafka.consumer.bootstrap.servers");
        String group = config.getString("kafka.consumer.group.id");
        String offsetReset = config.getString("kafka.consumer.auto.offset.reset");

        String producerServers = config.getString("kafka.producer.bootstrap.servers");

        //Kafka reader
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            KafkaReader kafkaReader = new KafReader("test_topic_in", consumerServers, group, offsetReset, db, producerServers);
            kafkaReader.processing();
        });
        executorService.shutdown(); // Завершить после выполнения все задач.
    }
}
