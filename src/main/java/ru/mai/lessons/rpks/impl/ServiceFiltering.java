package ru.mai.lessons.rpks.impl;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        // Считываем параметры конфигурации для подключения к базе данных
        Config db = config.getConfig("db");
        String url = db.getString("jdbcUrl");
        String user = db.getString("user");
        String password = db.getString("password");
        String driver = db.getString("driver");
        Long updateIntervalSec = config.getLong("application.updateIntervalSec");
        DbReader dbreader = new ReaderDB(url, user, password, driver);

        Queue<Rule[]> queue = new ConcurrentLinkedQueue<>();
        final int[] nRules = {1};
        new Thread(() -> {
            try {
                while(nRules[0] > 0) {
                    //Считываем правила из БД
                    Rule[] rules = dbreader.readRulesFromDB();
                    if(!queue.isEmpty()) queue.remove();
                    queue.add(rules);

                    nRules[0] = rules.length;
                    Thread.sleep(updateIntervalSec * 1000);
                }
            } catch (InterruptedException ex) {
                log.warn("Interrupted!", ex);
                Thread.currentThread().interrupt();
            }
        }).start();

        // Считываем параметры конфигурации для подключения к Kafka
        String consumerServers = config.getString("kafka.consumer.bootstrap.servers");
        String group = config.getString("kafka.consumer.group.id");
        String offsetReset = config.getString("kafka.consumer.auto.offset.reset");
        String topicIn = config.getString("kafka.consumer.topicIn");

        String producerServers = config.getString("kafka.producer.bootstrap.servers");
        String topicOut = config.getString("kafka.producer.topicOut");

        //Kafka reader
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            KafkaReader kafkaReader = new KafReader(topicIn, consumerServers, group, offsetReset, queue, producerServers, topicOut);
            kafkaReader.processing();
        });
        executorService.shutdown(); // Завершить после выполнения все задач.
    }
}
