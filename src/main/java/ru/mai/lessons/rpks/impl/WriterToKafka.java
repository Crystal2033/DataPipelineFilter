package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jooq.tools.json.ParseException;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.impl.settings.ProducerSettings;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Getter
@Setter
@Builder
public class WriterToKafka implements KafkaWriter {
    ProducerSettings producerSettings;
    private AtomicBoolean isExit;
    ConcurrentLinkedQueue<Message> concurrentLinkedQueue;
    ConcurrentLinkedQueue<Rule[]> rules;
    ProcessorOfRule processorOfRule;

    @Override
    public void processing(Message message) {
        try {
            assert rules.peek() != null;
            processorOfRule.processing(message, rules.peek());
        } catch (ParseException e) {
            log.warn("NOT_CORRECT_MASSAGE:" + message.getValue());
        }
    }

    void writeMessage(Message message) {
        log.debug("START_WRITE_MESSAGE_IN_KAFKA_TOPIC {}", producerSettings.getTopicOut());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, producerSettings.getBootstrapServers(),
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
        try (kafkaProducer) {
            kafkaProducer.send(new ProducerRecord<>(producerSettings.getTopicOut(), message.getValue()));
        }
    }

    void startWriter() {
        while (!isExit.get()) {
            if ((!concurrentLinkedQueue.isEmpty()) && !(rules.isEmpty())) {
                Message message = concurrentLinkedQueue.poll();
                log.debug("KAFKA_PRODUCER_START_PROCESSING_MASSAGE: " + message.getValue());
                processing(message);
                if (message.isFilterState()) {
                    writeMessage(message);
                    log.debug("KAFKA_PRODUCER_SEND_MASSAGE: " + message.getValue());
                }
            }
        }
    }
}
