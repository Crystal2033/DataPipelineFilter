package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.config.KafkaConfig;
import ru.mai.lessons.rpks.model.Message;

import java.util.UUID;
import java.util.concurrent.Future;

@Slf4j
public class KafkaWriterImpl implements KafkaWriter {
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicOut;

    public KafkaWriterImpl(Config config) {
        this.kafkaProducer = KafkaConfig.createProducer(config);
        this.topicOut = KafkaConfig.getTopicOut(config);
    }

    @SneakyThrows
    @Override
    public void processing(Message message) {
        if (!message.isFilterState()) {
            return;
        }
        log.debug("Sending message " + message.getValue());
        kafkaProducer.send(new ProducerRecord<>(topicOut,
            UUID.randomUUID().toString(),
            message.getValue())).get();
        log.debug("Sent message to output " + message.getValue());
    }
}
