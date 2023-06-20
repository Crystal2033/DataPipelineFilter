package ru.mai.lessons.rpks.impl;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

import java.util.List;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
public class KafkaReaderImpl implements KafkaReader {

    private final String topic;
    private final String bootstrapServers;
    private final String groupId;
    private final String autoOffsetReset;
    private final MessageHandler messageHandler;

    @Override
    public void processing() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        KafkaConsumer<String, String> consumer;

        try {
            consumer = new KafkaConsumer<>(config);
            consumer.subscribe(List.of(topic));

            while (true) {
                if (Thread.interrupted()) {
                    break;
                }
                else {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (var recordMsg : records)
                        messageHandler.processMessage(Message.builder().value(recordMsg.value()).filterState(false).build());
                }
            }

        } catch (WakeupException e) {
            log.error("Catch wake up exception");
        } catch (Exception e) {
            log.error("Catch unexpected exception", e);
        } finally {
            log.info("Consumer closed");
        }

    }
}