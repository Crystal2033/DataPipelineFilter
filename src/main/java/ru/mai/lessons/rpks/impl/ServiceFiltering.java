package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class ServiceFiltering implements Service {

    ConcurrentLinkedQueue<Message> messages = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Rule> rules;

    @Override
    public void start(Config config) {
        DBReaderImpl dbReaderImpl = new DBReaderImpl(config.getConfig("db"));

        Rule[] tempRules = dbReaderImpl.readRulesFromDB();

        rules = Arrays.stream(tempRules).collect(Collectors.toCollection(ConcurrentLinkedQueue<Rule>::new));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        config.getString("kafka.producer.topic.output");
        KafkaWriterImpl producer = new KafkaWriterImpl(config.getString("kafka.producer.topic.output"),
                config.getString("kafka.producer.bootstrap.servers"));

        RuleProcessorImpl ruleProcessor = new RuleProcessorImpl();

        ScheduledExecutorService schedulerExecutorService =  Executors.newScheduledThreadPool(1);

        RulesUpdaterThread rulesUpdaterThread = new RulesUpdaterThread(dbReaderImpl, config.getLong("application.updateIntervalSec"), rules);

        schedulerExecutorService.scheduleWithFixedDelay(rulesUpdaterThread, 0, config.getLong("application.updateIntervalSec"), TimeUnit.SECONDS);

        MessageHandler messageHandler = new MessageHandler(producer, ruleProcessor, rules, rulesUpdaterThread);

        KafkaReaderImpl consumer = new KafkaReaderImpl(config.getString("kafka.consumer.topic.enter"),
                config.getString("kafka.consumer.bootstrap.servers"),
                config.getString("kafka.consumer.group.id"),
                messageHandler);
        executorService.execute(consumer::processing);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("ShutdownHook schedulerExecutorService");
                schedulerExecutorService.shutdown();
                executorService.shutdown();
            }
        });

    }
}
