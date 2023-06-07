package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.MyException;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
public class ServiceFiltering implements Service {
    Db db;
    Rule[] rules;
    int updateIntervalSec;

    @Override
    public void start(Config config) {

        rules = new Rule[1];
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        updateIntervalSec = config.getInt("application.updateIntervalSec");
        db = new Db(config);
        rules = db.readRulesFromDB();
        String reader = config.getString("kafka.consumer.bootstrap.servers");
        String writer = config.getString("kafka.producer.bootstrap.servers");
        String topicIn = config.getString("kafka.topicIn");
        String topicOut = config.getString("kafka.topicOut");
        KafkaReaderImpl kafkaReader = new KafkaReaderImpl(topicIn, topicOut, config, reader, writer, rules);
        TimerTask task = new TimerTask() {
            public void run() {
                    rules = db.readRulesFromDB();
                    for (Rule r :
                            rules) {
                        log.info(r.toString());
                        log.info("TIMER");

                    }
                    kafkaReader.setRules(rules);
            }
        };

        Timer timer = new Timer(true);

        timer.schedule(task, 0, 1000L * updateIntervalSec);

        executorService.execute(() -> {
            kafkaReader.setRules(rules);
            kafkaReader.processing();
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                task.cancel();
                timer.cancel();
            }
        });

    }
}