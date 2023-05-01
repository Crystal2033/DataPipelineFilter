package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

@Slf4j
@Builder
public final class KafkaWriterImpl implements KafkaWriter {
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesGetter;
    private final String topic;
    private final String bootstrapServers;
    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void processing(Message message) {
        Message checkedMessage = ruleProcessor.processing(message, rulesGetter.get());
        if (checkedMessage.isFilterState())
            send(message);
    }

    private void send(Message message) {
        if (Optional.ofNullable(kafkaProducer).isEmpty())
            init();

        kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));
        log.info("Message send: {}", message);
    }

    private void init() {
        log.info("Init KafkaWriterImpl");

        kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }
}