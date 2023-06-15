package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.impl.settings.ConsumerSettings;
import ru.mai.lessons.rpks.model.Message;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Getter
@Setter
@Builder
public class ReaderFromKafka implements KafkaReader {
    private AtomicBoolean isExit;
    private ConsumerSettings consumerSettings;
    ConcurrentLinkedQueue<Message> concurrentLinkedQueue;
    @Override
    public void processing() {
        log.info("KAFKA_CONSUMER_START_READING_FROM_TOPIC {}", consumerSettings.getTopicIn());
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerSettings.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-"+UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerSettings.getAutoOffsetReset()
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
        kafkaConsumer.subscribe(Collections.singletonList(consumerSettings.getTopicIn()));
        try (kafkaConsumer) {
            while (!isExit.get()) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("MASSAGE_FROM_KAFKA_TOPIC {} : {}", consumerRecord.topic(), consumerRecord.value());
                    concurrentLinkedQueue.add(Message.builder().value(consumerRecord.value()).build());
                    if (consumerRecord.value().equals("$exit")) {
                        isExit.set(true);
                    } else {
                        log.info("MASSAGE_FROM_KAFKA_TOPIC {} : {}", consumerRecord.topic(), consumerRecord.value());
                    }
                }
            }
            log.info("READ_IS_DONE!");
        }
    }

}
