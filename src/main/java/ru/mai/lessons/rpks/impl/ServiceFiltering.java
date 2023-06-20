package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.Service;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.*;

@Slf4j
public class ServiceFiltering implements Service {
    Rule[] rules;

    @Override
    public void start(Config config) {

        MyDbReader myDbReader = new MyDbReader(
                config.getString("db.jdbcUrl"),
                config.getString("db.password"),
                config.getString("db.user")
        );

        rules = myDbReader.readRulesFromDB();

        MyKafkaReader myKafkaReader = new MyKafkaReader(
                config.getString("kafka.consumer.topic"),
                config.getString("kafka.producer.topic"),
                config.getString("kafka.consumer.bootstrap.servers"),
                config.getString("kafka.producer.bootstrap.servers"),
                rules
        );

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            rules = myDbReader.readRulesFromDB();
            myKafkaReader.setRules(rules);
        };

        int updateIntervalSec = config.getInt("application.updateIntervalSec");
        executorService.scheduleAtFixedRate(task, 0, updateIntervalSec, TimeUnit.SECONDS);
        myKafkaReader.processing();
        Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdown));
    }
}
