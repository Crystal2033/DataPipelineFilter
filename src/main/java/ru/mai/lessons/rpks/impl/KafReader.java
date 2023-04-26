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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafReader implements KafkaReader {
    private final String topic;
    private final String bootstrapServers;
    private final String group;
    private final String offsetReset;
    private final Config db;
    private final String producerServers;
    private boolean isExit;

    public void processing() {
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

                // Считываем параметры конфигурации для подключения к базе данных
                String url = db.getString("jdbcUrl");
                String user = db.getString("user");
                String password = db.getString("password");
                DbReader dbreader = new ReaderDB(url, user, password);
                Rule[] rules = dbreader.readRulesFromDB();

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
                    String jsonString =  consumerRecord.value();
                    if (Objects.equals(jsonString, "")) continue;

                    Message message = Message.builder().value(jsonString).filterState(false).build();
                    RuleProcess ruleProcess = new RuleProcess();
                    message = ruleProcess.processing(message, rules); // проверка сообщения
                    log.info("Message status {} : {}", message.getValue(), message.isFilterState());

                    if(message.isFilterState()){
                        KafkaWriter kafkaWriter = new KafWriter("test_topic_out", producerServers);
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
