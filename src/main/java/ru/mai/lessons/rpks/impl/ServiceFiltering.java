package ru.mai.lessons.rpks.impl;

import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import lombok.extern.slf4j.Slf4j;

import com.typesafe.config.Config;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        DbReaderImpl dbReaderImpl = new DbReaderImpl(config.getConfig("db"));

        config.getString("kafka.producer.topic.output");
        KafkaWriterImpl producer = new KafkaWriterImpl(config.getString("kafka.producer.topic.output"),
                config.getString("kafka.producer.bootstrap.servers"));

        RuleProcessorImpl ruleProcessor = new RuleProcessorImpl();

        try {
            ConcurrentLinkedQueue<Rule> rules = Arrays.stream(dbReaderImpl.readRulesFromDB()).collect(Collectors.toCollection(ConcurrentLinkedQueue<Rule>::new));
            RulesUpdaterImpl rulesUpdater = new RulesUpdaterImpl(dbReaderImpl, config.getLong("application.updateIntervalSec"), rules);

            ScheduledExecutorService schedulerExecutorService =  Executors.newScheduledThreadPool(1);
            schedulerExecutorService.scheduleWithFixedDelay(rulesUpdater, 0, config.getLong("application.updateIntervalSec"), TimeUnit.SECONDS);

            ExecutorService executorService = Executors.newFixedThreadPool(2);

            MessageHandler messageHandler = new MessageHandler(producer, ruleProcessor, rules, rulesUpdater);

            KafkaReaderImpl consumer = new KafkaReaderImpl(config.getString("kafka.consumer.topic.enter"),
                    config.getString("kafka.consumer.bootstrap.servers"),
                    config.getString("kafka.consumer.group.id"),
                    config.getString("kafka.consumer.auto.offset.reset"),
                    messageHandler);

            executorService.execute(consumer::processing);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    schedulerExecutorService.shutdown();
                    executorService.shutdown();
                }
            });

        }  catch (IllegalStateException e) {
            e.printStackTrace();
        }
    }
}
