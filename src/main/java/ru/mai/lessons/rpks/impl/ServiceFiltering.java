package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;

@Slf4j
public class ServiceFiltering implements Service {
    private Rule[] rules;


    private DataBaseReader dataBaseReader;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    final Object locker = new Object();
    @Override
    public void start(Config config) {
        KafkaReader reader;
        KafkaWriter writer;
        dataBaseReader = new DataBaseReader(config);
        executor.scheduleAtFixedRate(this::updateRules, 0, config.getConfig("application")
                .getInt("updateIntervalSec"), TimeUnit.SECONDS);
        writer = new KafkaWriterI(config.getConfig("kafka"));
        reader = new KafkaReaderI(config.getConfig("kafka"), writer, new RuleProcessorI(), this::getRules);
        reader.processing();
    }

    private void updateRules(){
        try {
            synchronized (locker) {
                rules = dataBaseReader.readRulesFromDB();
                log.info("Rule was readed!");
            }
        } catch (java.sql.SQLException e) {
            log.error("Can't read rules......");
        }
    }

    private Rule[] getRules(){
        if (isNull(rules))
            updateRules();

        return rules;
    }

}
