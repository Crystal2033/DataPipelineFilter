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

import static ru.mai.lessons.rpks.impl.Db.closeConnection;
import static ru.mai.lessons.rpks.impl.Db.getConnection;
@Slf4j
public class ServiceFiltering implements Service {
    Db db;
    Rule[] rules;
    int updateIntervalSec;
    ConcurrentLinkedQueue<Message> queue;

    @Override
    public void start(Config config) {
        rules = new Rule[1];
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        ConfigurationReader configurationReader = new ConfigurationReader();
        updateIntervalSec = configurationReader.loadConfig().getInt("application.updateIntervalSec");
        queue = new ConcurrentLinkedQueue<>();
        db = new Db();
        Db.setUsername(configurationReader.loadConfig().getString("db.user"));
        Db.setPassword(configurationReader.loadConfig().getString("db.password"));
        Db.setJdbcUrl(configurationReader.loadConfig().getString("db.jdbcUrl"));
        Db.setDriverClassName(configurationReader.loadConfig().getString("db.driver"));
        Connection conn = null;
        try {
            conn = getConnection();
        } catch (SQLException e) {
            log.error("Error occurred while getting connection");
        }

        DSLContext context = DSL.using(conn, SQLDialect.POSTGRES);
        rules = db.readRulesFromDB(context);

        updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");
        TimerTask task = new TimerTask() {
            public void run() {
                rules = db.readRulesFromDB(context);
                for (Rule r :
                        rules) {
                    log.info(r.toString());

                }
                try {
                    closeConnection();
                    log.info("CONNECTION IS CLOSED");
                } catch (SQLException e) {
                    throw new MyException("Error while closing connection", e);
                }
            }
        };

        Timer timer = new Timer(true);
        timer.schedule(task, 0, 1000L * updateIntervalSec);
        log.info("delay:" + updateIntervalSec);


        String reader = configurationReader.loadConfig().getString("kafka.consumer.bootstrap.servers");
        KafkaReaderImpl kafkaReader = new KafkaReaderImpl("test_topic_in", "test_topic_out", reader, rules);
        executorService.execute(() -> {
            queue = kafkaReader.getQueue();
            log.info("+++++++" + queue);
        });


        executorService.execute(() -> {
            kafkaReader.setRules(rules);
            kafkaReader.processing();
        });


    }
}