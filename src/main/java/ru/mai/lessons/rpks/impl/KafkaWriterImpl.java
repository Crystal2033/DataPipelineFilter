package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import javax.management.ConstructorParameters;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

@Builder
@Slf4j
public class KafkaWriterImpl implements KafkaWriter {
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesGetter;
    private final String topic;
    private final String bootstrapServers;
    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void processing(Message message) {
        log.info("Process %s".formatted(message.getValue()));
        send(message);
    }

    private void send(Message message) {
        if (kafkaProducer == null)
            init();
        kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));
    }

    private void init() {
        log.info("Create Kafka Writer");

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
