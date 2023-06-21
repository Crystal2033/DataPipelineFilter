package ru.mai.lessons.rpks.impl;

import lombok.AllArgsConstructor;
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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@AllArgsConstructor
public final class KafkaWriterImpl implements KafkaWriter {
    private final String topic;
    private final String bootstrapServers;
    private final RuleProcessor ruleChecker;
    private final Rule[] rules;

    private KafkaProducer<String, String> kafkaProducer;

    @Override
    public void processing(Message message) {
        Message checkedMessage = ruleChecker.processing(message, rules);
        if (checkedMessage.isFilterState()) {
            if (Optional.ofNullable(kafkaProducer).isEmpty()) {
                log.info("init writerImpl");

                kafkaProducer = new KafkaProducer<>(
                        Map.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );
            }

            kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));
            log.info("Message sent {}", message);
        }
    }

}
