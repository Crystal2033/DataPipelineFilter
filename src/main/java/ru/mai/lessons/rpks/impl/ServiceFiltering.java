package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.Objects.isNull;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceFiltering implements Service {
    private Rule[] rules;
    private DbReaderImpl dataBaseReader;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    final Object locker = new Object();
    @Override
    public void start(Config config) {
        log.info("start");
        KafkaReaderImpl reader;
        dataBaseReader = new DbReaderImpl(config);
        log.info("created new db reader");
        executor.scheduleAtFixedRate(this::updateRules, 0, config.getConfig("application")
                .getInt("updateIntervalSec"), TimeUnit.SECONDS);
        String bootstrapServers = config.getString("kafka.consumer.bootstrap.servers");
        String bootstrapServersOut = config.getString("kafka.producer.bootstrap.servers");
        String groupId = config.getString("kafka.consumer.group.id");
        String autoOffsetReset = config.getString("kafka.consumer.auto.offset.reset");
        String topic = config.getString("kafka.consumer.topic");
        String topicOut = config.getString("kafka.producer.topic");

        log.info("bs");
        reader = new KafkaReaderImpl(bootstrapServers,
                bootstrapServersOut,
                groupId,
                autoOffsetReset,
                topic,
                topicOut,
                this::getRules, rules);
        log.info("created new kafka reader");
        reader.processing();

    }

    private void updateRules(){

        synchronized (locker) {
            rules = dataBaseReader.readRulesFromDB();
            log.debug("Rule was read!");
        }
    }

    private Rule[] getRules(){
        if (isNull(rules))
            updateRules();

        return rules;
    }
}
