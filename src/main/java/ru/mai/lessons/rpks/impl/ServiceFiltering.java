package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.*;

import static java.util.Objects.isNull;

@Slf4j
public class ServiceFiltering implements Service {
    private Rule[] rules;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private DatabaseReader databaseReader;
    private final Semaphore semaphoreDB = new Semaphore(1);

    private void updateRules() {
        try {
            semaphoreDB.acquire();
            rules = databaseReader.readRulesFromDB();
            semaphoreDB.release();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private Rule[] getRules() {
        if (isNull(rules)) {
            updateRules();
        }
        return rules;
    }

    @Override
    public void start(Config config) {
        databaseReader = new DatabaseReader(config.getConfig("db"));
        executor.scheduleAtFixedRate(this::updateRules, 0,
                config.getInt("application.updateIntervalSec"), TimeUnit.SECONDS);

        MyKafkaWriter kafkaWriter = new MyKafkaWriter(config.getConfig("kafka.producer"));

        MyKafkaReader kafkaReader = new MyKafkaReader(config.getConfig("kafka.consumer"),
                kafkaWriter, new MessageRuleProcessor(), this::getRules);

        kafkaReader.processing();
    }
}
