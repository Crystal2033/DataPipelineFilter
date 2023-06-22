package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.Service;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.*;




@Slf4j
public class ServiceFilteringClass implements Service {
    Rule[] rules;
    private DbReaderClass dbReaderClass;
    private KafkaReaderClass kafkaReaderClass;

    public void updateRules() {
        rules = dbReaderClass.readRulesFromDB();
        kafkaReaderClass.setRules(rules);
    }

    @Override
    public void start(Config config) {
        dbReaderClass = new DbReaderClass(config.getString("db.jdbcUrl"),
                config.getString("db.password"),
                config.getString("db.user"));
        rules = dbReaderClass.readRulesFromDB();
        kafkaReaderClass = new KafkaReaderClass(config.getString("kafka.consumer.topic"),
                config.getString("kafka.producer.topic"),
                config.getString("kafka.consumer.bootstrap.servers"),
                config.getString("kafka.producer.bootstrap.servers"),
                config.getString("kafka.consumer.group.id"),
                config.getString("kafka.consumer.auto.offset.reset"),
                rules);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this::updateRules, 0,
                config.getInt("application.updateIntervalSec"),
                TimeUnit.SECONDS);
        kafkaReaderClass.processing();
    }
}