package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;


@Slf4j
@Builder
public final class KafkaReaderImpl implements KafkaReader {
    private final KafkaWriter kafkaWriter;
    private final String topic;
    private final String kafkaOffset;
    private final String groupId;
    private final String bootstrapServers;

    private boolean isExit;

    @Override
    public void processing() {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaOffset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        kafkaConsumer.subscribe(Collections.singletonList(topic));

        log.info("init readerImpl");

        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    if (consumerRecord.value().equals("$exit")) {
                        isExit = true;
                    } else {
                        log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
                        kafkaWriter.processing(new Message(consumerRecord.value()));
                    }
                }
            }
            log.info("Read is done!");
        }
    }
}
