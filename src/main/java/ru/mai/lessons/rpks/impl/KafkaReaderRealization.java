package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.scheduler.RulesScheduler;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Setter
@SuppressWarnings("InfiniteLoopStatement")
public class KafkaReaderRealization implements KafkaReader {


    private List<Rule> ruleList;
    private KafkaConsumer<String, String> kafkaConsumer;
    public Config config;
    @Override
    public void processing() {
        createKafkaConsumer();

        // Run DataBase reader
        RulesScheduler rulesScheduler = new RulesScheduler();
        try {
            rulesScheduler.runScheduler(config);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        log.info("Start consumer cycle");
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
            ruleList = rulesScheduler.getRules();

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                Message curMessage = Message.builder().value(consumerRecord.value()).build();
//                if (ruleProcessor.processing(curMessage, curRules).isFilterState()) {
//                    log.info("Message " + curMessage.getValue() + " accepted");
//                    kafkaWriter.processing(curMessage);
//                } else {
//                    log.info("Message " + curMessage.getValue() + " rejected");
//                }
            }
        }
    }

    public void createKafkaConsumer() {
        log.info("Create new consumer");

        if (config == null) {
            //throw exception
        }

        Properties properties = new Properties();
        properties.put("group.id", config.getString("kafka.consumer.group.id"));
        properties.put("bootstrap.servers", config.getString("kafka.consumer.bootstrap.servers"));
        properties.put("auto.offset.reset", config.getString("kafka.consumer.auto.offset.reset"));

        kafkaConsumer = new KafkaConsumer<>(properties);
    }
}
