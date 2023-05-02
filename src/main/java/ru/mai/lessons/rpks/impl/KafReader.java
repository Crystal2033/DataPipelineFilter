package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.tools.json.*;
import ru.mai.lessons.rpks.ConfigReader;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.*;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafReader implements KafkaReader {
    private final String topic;
    private final String bootstrapServers;
    private final String group;
    private final String offsetReset;
    private final Queue<Rule[]> queue;
    private final String producerServers;
    private final String topicOut;
    private boolean isExit;


    public void processing() {
        RuleProcess ruleProcess = new RuleProcess();
        KafkaWriter kafkaWriter = new KafWriter(topicOut, producerServers);

        log.info("Start reading kafka topic {}", topic);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, group,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        kafkaConsumer.subscribe(Collections.singletonList(topic));

        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    String jsonString =  consumerRecord.value();
                    if (jsonString.isBlank()) continue;

                    Message message = Message.builder().value(jsonString).filterState(false).build();

                    Rule[] rules = queue.peek();
                    assert rules != null;
                    message = ruleProcess.processing(message, rules); // проверка сообщения

                    if(message.isFilterState()){
                        kafkaWriter.processing(message);
                    }
                }
            }
            log.info("Read is done!");
        } catch (ParseException e) {
            log.warn("ParseException!", e);
        }
    }
}
