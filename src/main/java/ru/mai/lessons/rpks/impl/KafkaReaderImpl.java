package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public final class KafkaReaderImpl implements KafkaReader {
    private final KafkaWriter kafkaWriter;
    private final Supplier<Rule[]> rulesGetter;
    private final RuleProcessor ruleProcessor;
    private final String topic;
    private final String kafkaOffset;
    private final String groupId;
    private final String bootstrapServers;
    private KafkaConsumer<String, String> kafkaConsumer;

    @Override
    @SuppressWarnings("InfiniteLoopStatement")
    public void processing() {
        log.info("Start Kafka reader");

        kafkaConsumer = new KafkaConsumer<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.bootstrapServers, ConsumerConfig.GROUP_ID_CONFIG,
                this.groupId, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                this.kafkaOffset), new StringDeserializer(), new StringDeserializer());
        log.info("Kafka consumer created");
        log.info("Set kafka topic : " + topic);
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            log.info("Start consumer reading");
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
                Rule[] curRules = rulesGetter.get();
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    Message curMessage = Message.builder().value(consumerRecord.value()).build();
                    if (ruleProcessor.processing(curMessage, curRules).isFilterState()) {
                        kafkaWriter.processing(curMessage);
                    }
                }
            }
        });
    }
}