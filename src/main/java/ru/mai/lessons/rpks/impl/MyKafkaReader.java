package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
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

@Slf4j
@Getter
@Setter
public class MyKafkaReader implements KafkaReader {
    private Config config;
    @NonNull
    private Rule[] rules;
    KafkaConsumer<String, String> kafkaConsumer;

    public MyKafkaReader(Config config, Rule[] rules){
        this.config = config;
        this.rules = rules;
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.consumer.bootstrap.servers"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumer.group.id"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("kafka.consumer.auto.offset.reset"));
        kafkaConsumer = new KafkaConsumer<>(props);

        kafkaConsumer.subscribe(Collections.singletonList(config.getString("kafka.consumer.topic")));
    }
    @Override
    public void processing() {
        MyRuleProcessor myRuleProcessor = new MyRuleProcessor();
        MyKafkaWriter myKafkaWriter = new MyKafkaWriter(
                config.getString("kafka.producer.topic"),
                config.getString("kafka.producer.bootstrap.servers")
        );

        try{

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    Message message = new Message(consumerRecord.value(), true);
                    message = myRuleProcessor.processing(message, rules);
                    if (message.isFilterState()) {
                        myKafkaWriter.processing(message);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Create consumer exception", e);
        }
    }

}