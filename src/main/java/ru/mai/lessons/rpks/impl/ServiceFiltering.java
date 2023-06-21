package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.*;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceFiltering implements Service {

    private Rule[] rules;
    private DbReader dbReader;
    private final RuleProcessor ruleChecker = new RuleProcessorImpl();

    private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
    private final Object synchronizer = new Object();

    @Override
    public void start(Config config) {
        dbReader = new DbReaderImpl(config);

        Config prodConf = config.getConfig("kafka").getConfig("producer");
        Config consConf = config.getConfig("kafka").getConfig("consumer");
        Config appConf = config.getConfig("application");

        exec.scheduleAtFixedRate(this::rulesUpdater,
                0,
                Integer.parseInt(appConf.getString("updateIntervalSec")),
                TimeUnit.SECONDS
        );

        rules = readDbRules();

        KafkaWriter kafkaWriter = new KafkaWriterImpl(prodConf.getString("topic"),
                prodConf.getString("bootstrap.servers"),
                ruleChecker, rules);

        KafkaReader kafkaReader = new KafkaReaderImpl(consConf.getString("topic"),
                consConf.getString("bootstrap.servers"),
                kafkaWriter,
                consConf.getString("auto.offset.reset"),
                consConf.getString("group.id"));

        kafkaReader.processing();
    }

    private void rulesUpdater() {
        synchronized (synchronizer) {
            rules = dbReader.readRulesFromDB();
            log.debug("Now in synchronized");
        }
        log.info("Read rules from DB");
    }

    private Rule[] readDbRules() {
        synchronized (synchronizer) {
            log.debug("Now in synchronized");
            Rule[] ruleDB = null;
            if (rules == null) {
                ruleDB = dbReader.readRulesFromDB();
            }
            log.info("Read rules from DB");
            return ruleDB;
        }

    }
}
