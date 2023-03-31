package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class ServiceFiltering implements Service {
    private static final String DATA_BASE_CONFIG_NAME = "db";
    private static final String RULE_INTERVAL_CONFIG_NAME = "application";
    private static final String TOPIC = "";
    private static final String BOOTSTRAP_SERVICES = "";

    private Rule[] rules;
    private DbReader dbReader;
    private final RuleProcessor ruleProcessor = new RuleProcessorImpl();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final Object lock = new Object();

    @Override
    public void start(Config config) {
        dbReader = new DbReaderImpl(config.getConfig(DATA_BASE_CONFIG_NAME));
        initScheduledExecutorServiceForRuleUpdate(config.getConfig(RULE_INTERVAL_CONFIG_NAME));

        KafkaWriter kafkaWriter = KafkaWriterImpl.builder()
                .ruleProcessor(ruleProcessor)
                .rulesGetter(this::getRules)
                .topic(TOPIC)
                .bootstrapServers(BOOTSTRAP_SERVICES)
                .build();

        KafkaReader kafkaReader = KafkaReaderImpl.builder()
                .kafkaWriter(kafkaWriter)
                .bootstrapServers(TOPIC)
                .topic(BOOTSTRAP_SERVICES)
                .build();

        kafkaReader.processing();
    }

    private void initScheduledExecutorServiceForRuleUpdate(Config ruleIntervalConfig) {
        final String updateIntervalFiledName = "updateIntervalSec";
        String interval = ruleIntervalConfig.getString(updateIntervalFiledName);

        executorService.scheduleAtFixedRate(
                this::updateRules,
                0,
                Integer.parseInt(interval),
                TimeUnit.SECONDS
        );
    }

    private void updateRules() {
        synchronized (lock) {
            rules = dbReader.readRulesFromDB();
        }
    }

    private Rule[] getRules() {
        synchronized (lock) {
            if (rules == null) {
                rules = dbReader.readRulesFromDB();
            }
            return rules;
        }
    }


}
