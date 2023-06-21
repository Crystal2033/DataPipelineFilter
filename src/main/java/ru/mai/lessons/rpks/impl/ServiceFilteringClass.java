package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.Service;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.*;

@Slf4j
public class ServiceFilteringClass implements Service {
    Rule[] rules;

    @Override
    public void start(Config config) {
        String dbJdbcUrl = config.getString("db.jdbcUrl");
        String dbUser = config.getString("db.user");
        String dbPassword = config.getString("db.password");


        DbReaderClass dbReaderClass = new DbReaderClass(dbJdbcUrl, dbPassword, dbUser);
        rules = dbReaderClass.readRulesFromDB();

        String consumerTopic = config.getString("kafka.consumer.topic");
        String producerTopic = config.getString("kafka.producer.topic");
        String consumerBootstrapServers = config.getString("kafka.consumer.bootstrap.servers");
        String producerBootstrapServers = config.getString("kafka.producer.bootstrap.servers");

        KafkaReaderClass kafkaReaderClass = new KafkaReaderClass(consumerTopic, producerTopic, consumerBootstrapServers, producerBootstrapServers, rules);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        Runnable task = () -> {
            rules = dbReaderClass.readRulesFromDB();
            kafkaReaderClass.setRules(rules);
        };
        int updateIntervalSec = config.getInt("application.updateIntervalSec");
        executorService.scheduleAtFixedRate(task, 0, updateIntervalSec, TimeUnit.SECONDS);
        kafkaReaderClass.processing();
        Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdown));
    }
}