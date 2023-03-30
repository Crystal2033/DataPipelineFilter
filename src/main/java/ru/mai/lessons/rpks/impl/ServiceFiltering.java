package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

public final class ServiceFiltering implements Service {
    private DbReader dbReader;
    private Rule[] rules;
    private final Object lock = new Object();

    @Override
    public void start(Config config) {
        dbReader = new DbReaderImpl(config.getConfig("db"));
        RuleProcessor ruleProcessor = new RuleProcessorImpl();

        // TODO: config
        KafkaWriter kafkaWriter = new KafkaWriterImpl(ruleProcessor, this::getRules);

        // TODO: config
        KafkaReader kafkaReader = new KafkaReaderImpl(kafkaWriter);
        kafkaReader.processing();

        // написать код реализации сервиса фильтрации
    }


    private Rule[] getRules() {
        synchronized (lock) {
            if (rules == null) {
                rules = dbReader.readRulesFromDB();
            }
            return rules;
        }
    }

    // TODO: надо вызывать в executor
    private void updateRules() {
        synchronized (lock) {
            rules = dbReader.readRulesFromDB();
        }
    }
}
