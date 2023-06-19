package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
@Slf4j
public class ServiceFiltering implements Service {
    private static final String RULE_CONFIG = "application";
    private static final String DB_CONFIG = "db";
    private static final String KAFKA_CONFIG = "kafka";
    private Rule[] rules;
    private DbReader dbReader;
    private final RuleProcessor ruleProcessor = new RuleProcessorImpl();
    private final Object dbLock = new Object();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private Rule[] getRules() {
        synchronized (dbLock) {
            if (rules == null) {
                try {
                    rules = dbReader.readRulesFromDB();
                }  catch (IllegalStateException ignored) {
                    rules = null;
                }
            }
            return rules;
        }
    }

    private void updateRules() {
        log.info("start update db");
        synchronized (dbLock) {
            rules = dbReader.readRulesFromDB();
        }
        log.info("end update db");
    }
    @Override
    public void start(Config config) {
        Config dbConfig = config.getConfig(DB_CONFIG);
        Config ruleConfig = config.getConfig(RULE_CONFIG);
        Config producerConfig = config.getConfig(KAFKA_CONFIG).getConfig("producer");
        Config consumerConfig = config.getConfig(KAFKA_CONFIG).getConfig("consumer");

        dbReader = new DbReaderImpl(dbConfig);
        executor.scheduleAtFixedRate(
                this::updateRules,
                0,
                Integer.parseInt(ruleConfig.getString("updateIntervalSec")),
                TimeUnit.SECONDS
        );


        KafkaWriter writer = KafkaWriterImpl.builder()
                .ruleProcessor(ruleProcessor)
                .rulesGetter(this::getRules)
                .topic(producerConfig.getString("topic"))
                .bootstrapServers(producerConfig.getString("bootstrap.servers"))
                .build();


        KafkaReader reader = KafkaReaderImpl.builder()
                .kafkaWriter(writer)
                .topic(consumerConfig.getString("topic"))
                .bootstrapServers(consumerConfig.getString("bootstrap.servers"))
                .kafkaOffset(consumerConfig.getString("auto.offset.reset"))
                .groupId(consumerConfig.getString("group.id"))
                .build();

        reader.processing();
    }
}

