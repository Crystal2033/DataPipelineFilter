package ru.mai.lessons.rpks.impl;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
@AllArgsConstructor
public final class KafkaReaderImpl implements KafkaReader {

    private final String topic;
    private final String bootstrapServers;
    private final KafkaWriter kafkaWriter;
    private final String kafkaOffset;
    private final String groupId;

    private KafkaConsumer<String, String> kafkaConsumer;
    private ExecutorService executor;

    @Override
    public void processing() {
        log.info("Start reading kafka topic {}", topic);
        kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, this.groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaOffset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        kafkaConsumer.subscribe(Collections.singletonList(topic));

        executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    kafkaWriter.processing(new Message(consumerRecord.value()));
                }
            }
        });
        executor.shutdown();
    }
}
