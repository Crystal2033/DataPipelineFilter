package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceFiltering implements Service {
    private Rule[] rules;
    private DbReader dbReader;
    private final RuleProcessor ruleProcessor = new RuleProcessorImpl();
    private final Object locker = new Object();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);


    private void loadRules() {
        log.info("Start loading rules from DB");
        synchronized (locker) {
            rules = Arrays.stream(dbReader.readRulesFromDB()).filter(rule ->
                    rule.getFilterValue() != null &&
                            !rule.getFilterValue().isEmpty() &&
                            rule.getFilterFunctionName() != null &&
                            !rule.getFilterFunctionName().isEmpty() &&
                            !rule.getFilterFunctionName().isBlank() &&
                            rule.getFieldName() != null &&
                            !rule.getFieldName().isEmpty() &&
                            !rule.getFieldName().isBlank()
            ).map(rule -> {
                rule.setFilterFunctionName(rule.getFilterFunctionName().toUpperCase());
                return rule;
            }).toArray(Rule[]::new);
            log.info("Read " + rules.length + " rules");
        }
        log.info("End load rules from DB");
    }

    private Rule[] getRules() {
        Rule[] resRules;
        synchronized (locker) {
            resRules = rules.clone();
        }
        return resRules;
    }

    @Override
    public void start(Config config) {
        dbReader = new DbReaderImpl(config.getConfig("db"));
        executor.scheduleAtFixedRate(
                this::loadRules,
                0,
                Integer.parseInt(config.getConfig("application").getString("updateIntervalSec")),
                TimeUnit.SECONDS
        );

        Config kafka = config.getConfig("kafka");
        Config producer = kafka.getConfig("producer");

        KafkaWriter kafkaWriter = new KafkaWriterImpl(
                producer.getString("topic"),
                producer.getString("bootstrap.servers"));

        Config consumer = kafka.getConfig("consumer");
        KafkaReader kafkaReader = new KafkaReaderImpl(kafkaWriter,
                this::getRules,
                ruleProcessor,
                consumer.getString("topic"),
                consumer.getString("auto.offset.reset"),
                consumer.getString("group.id"),
                consumer.getString("bootstrap.servers"));

        kafkaReader.processing();
    }
}
