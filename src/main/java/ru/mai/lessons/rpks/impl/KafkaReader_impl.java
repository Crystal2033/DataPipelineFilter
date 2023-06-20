package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class KafkaReader_impl implements KafkaReader {
    private Rule[] rules;
    private final DbReader db;
    private final Timer timer = new Timer();

    private final TimerTask updateRulesTask = new TimerTask() {
        @Override
        public void run() {
            updateRulesFromDb();
        }
    };

    private final Config appConfig;
    private final RuleProcessor_impl ruleProcessor;
    private final KafkaWriter_impl producer;

    public KafkaReader_impl(Config config) {
        appConfig = config;
        db = new DbReader_impl(config);
        rules = db.readRulesFromDB();
        logRules();

        ruleProcessor = new RuleProcessor_impl();
        producer = new KafkaWriter_impl(appConfig);

        timer.schedule(updateRulesTask, 0, appConfig.getLong("application.updateIntervalSec") * 1000);
    }

    private void updateRulesFromDb() {
        rules = db.readRulesFromDB();
        logRules();
    }

    private void logRules() {
        if (rules.length > 0) {
            StringBuilder stringBuilder = new StringBuilder("Loaded rules: ");
            for (Rule rule : rules) {
                stringBuilder.append(rule.toString()).append(", ");
            }
            String loadedRules = stringBuilder.substring(0, stringBuilder.length() - 2);
            log.info(loadedRules);
        } else {
            log.info("No rules loaded.");
        }
    }

    @Override
    public void processing() {
        Properties config = new Properties();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put("bootstrap.servers", appConfig.getString("kafka.consumer.bootstrap.servers"));
        config.put("group.id", appConfig.getString("kafka.consumer.group.id"));
        config.put("auto.offset.reset", appConfig.getString("kafka.consumer.auto.offset.reset"));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            log.info("Connecting to topic: {}", appConfig.getString("kafka.consumer.topic"));
            consumer.subscribe(Collections.singletonList(appConfig.getString("kafka.consumer.topic")));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var localRecord : records) {
                    log.info("Received message: {}", localRecord.value());
                    var msg = ruleProcessor.processing(new Message(localRecord.value(), false), rules);
                    if (msg.isFilterState()) {
                        log.info("Sending message: {}", msg.getValue());
                        producer.processing(msg);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Kafka exception: {}", e.toString());
        }
    }
}
