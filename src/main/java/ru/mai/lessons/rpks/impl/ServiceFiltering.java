package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.settings.ConsumerSettings;
import ru.mai.lessons.rpks.impl.settings.DBSettings;
import ru.mai.lessons.rpks.impl.settings.ProducerSettings;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ServiceFiltering implements Service {

    @Override
    public void start(Config config) {
        log.info("CONFIG:"+config.toString());
        AtomicBoolean isExit=new AtomicBoolean(false);
        ConcurrentLinkedQueue<Message> concurrentLinkedQueue=new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Rule[]> rules=new ConcurrentLinkedQueue<>();
        ExecutorService executorService= Executors.newFixedThreadPool(2);

        ConsumerSettings consumerSettings=Settings.makeConsumerSettings(config);
        ProducerSettings producerSettings=Settings.makeProducerSettings(config);
        DBSettings dbSettings=Settings.makeDBSettings(config);
        int updateIntervalSec=config.getConfig("application").getInt("updateIntervalSec");

        WriterToKafka writerToKafka= WriterToKafka.builder().producerSettings(producerSettings)
                .concurrentLinkedQueue(concurrentLinkedQueue).rules(rules).build();
        ReaderFromKafka readerFromKafka= ReaderFromKafka.builder().consumerSettings(consumerSettings)
                .concurrentLinkedQueue(concurrentLinkedQueue).isExit(isExit).build();

        executorService.submit(writerToKafka::startWriter);
        executorService.submit(readerFromKafka::processing);
        ReaderFromDB readerFromDB=ReaderFromDB.builder().dbSettings(dbSettings).build();
        while(!isExit.get()) {
            rules.add(readerFromDB.readRulesFromDB());
            log.info("ADD_RULE");
            if(rules.size()>1) {
                rules.poll();
            }
            try {
                Thread.sleep(updateIntervalSec* 1000L);
            } catch (InterruptedException e) {
                log.warn("CANT_SLEEP:"+e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
        executorService.shutdown();
        // написать код реализации сервиса фильтрации
    }
    private  record Settings() {
        private static ConsumerSettings makeConsumerSettings(Config config) {
            Config kafkaConfigConsumer = config.getConfig("kafka").getConfig("consumer");
            ConsumerSettings consumerSettings = ConsumerSettings.builder()
                    .groupId(kafkaConfigConsumer.getString("group.id"))
                    .bootstrapServers(kafkaConfigConsumer.getString("bootstrap.servers"))
                    .autoOffsetReset(kafkaConfigConsumer.getString("auto.offset.reset"))
                    //.updateIntervalSec(config.getConfig("application").getInt("updateIntervalSec"))
                    .updateIntervalSec(100)
                    .topicIn(config.getConfig("kafka").getString("topicIn")).build();
            log.info("CONSUMER_SETTINGS_WAS_READ: " + consumerSettings.toString());
            return consumerSettings;
        }

        private static  ProducerSettings makeProducerSettings(Config config) {
            Config kafkaConfigProducer = config.getConfig("kafka").getConfig("producer");
            ProducerSettings producerSettings = ProducerSettings.builder()
                    .bootstrapServers(kafkaConfigProducer.getString("bootstrap.servers"))
                    //.updateIntervalSec(config.getConfig("application").getInt("updateIntervalSec"))
                    .updateIntervalSec(100)
                    .topicOut(config.getConfig("kafka").getString("topicOut")).build();
            log.info("PRODUCER_SETTINGS_WAS_READ: " + producerSettings.toString());
            return producerSettings;
        }

        private static DBSettings makeDBSettings(Config config) {
            Config dbConfig = config.getConfig("db");
            DBSettings dbSettings = DBSettings.builder().jdbcUrl(dbConfig.getString("jdbcUrl"))
                    .driver(dbConfig.getString("driver"))
                    .user(dbConfig.getString("user"))
                    .password(dbConfig.getString("password"))
                   // .updateIntervalSec(config.getConfig("application").getInt("updateIntervalSec"))
                    .tableName("filter_rules").build();
            log.info("DB_SETTINGS_WAS_READ: " + dbSettings.toString());
            return dbSettings;
        }
    }
}
