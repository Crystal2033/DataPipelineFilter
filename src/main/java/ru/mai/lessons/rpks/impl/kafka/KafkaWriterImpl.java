package ru.mai.lessons.rpks.impl.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Builder
@AllArgsConstructor
public class KafkaWriterImpl implements KafkaWriter {
    private final String topic;
    private final String bootstrapServers;
    private KafkaProducer<String, String> kafkaProducer = null;

    @Override
    public void processing(Message message) {
        if (kafkaProducer == null) {
            initKafkaReader();
        }

        if (message.isFilterState()) {
            Future<RecordMetadata> response = null;

            response = kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()));
            Optional.ofNullable(response).ifPresent(rsp -> {
                try {
                    log.info("Message send {}", rsp.get());
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error sending message ", e);
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    private void initKafkaReader() {
        log.info("Start write message in kafka topic {}", topic);
        kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }
}
