package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
@Data
public class MyKafkaReader implements KafkaReader {
    private Rule[] rules;
    private Config config;
    private MyDbReader db;

    private String subscribeString;
    private long updateIntervalSec;
    @Override
    public void processing() {
        Properties properties = new Properties();
        MyKafkaWriter kafkaWriter = new MyKafkaWriter();
        kafkaWriter.setConfig(config);
        MyRuleProcessor processor = new MyRuleProcessor();

        db = new MyDbReader(config.getConfig("db"));
        rules = db.readRulesFromDB();
        updateIntervalSec = config.getConfig("application").getInt("updateIntervalSec");
        TimerTask task = new TimerTask() {
            public void run() {
                log.info("asdfghj");
                rules = db.readRulesFromDB();
                for (Rule r:
                     rules) {
                    log.info(r.toString());
                }
            }
        };

        Timer timer = new Timer(true);
        timer.schedule(task, 0, 1000*updateIntervalSec);
        log.info("delay:" + updateIntervalSec);


        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConfig("kafka").getConfig("consumer").getConfig("bootstrap").getString("servers"));
        log.info(config.getConfig("kafka").getConfig("consumer").getConfig("bootstrap").getString("servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_consumer");


        Message message;


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(Collections.singleton("test_topic_in"));
            ConsumerRecords<String, String> records;
            Thread.sleep(750);

            do {
                records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    continue;
                } else {
                    log.info(subscribeString + ": i got some : " + records.count());
                }


                for (ConsumerRecord<String, String> consumerRecord : records) {
                    message = processor.processing(new Message(consumerRecord.value(), true), rules);
                    log.info("message is: " + message.getValue() + ":" + message.isFilterState() );
                    log.info("some8");
                    if(message.isFilterState()) {
                        log.info("i try send " + message.getValue());
                        kafkaWriter.processing(message);
                    }
                }

            } while (true);
        }catch (InterruptedException e){
            log.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
