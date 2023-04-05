package ru.mai.lessons.rpks.impl.kafka;

import lombok.RequiredArgsConstructor;
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
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@RequiredArgsConstructor
public class KafkaWriterImpl implements KafkaWriter {
    private final String topic;
    private final String bootstrapServers;
    @Override
    public void processing(Message message) {
        log.info("Start write message in kafka topic {}", topic);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );

        try (kafkaProducer;
             Scanner scanner = new Scanner(System.in)) {
            String inputData;
            do {
                inputData = scanner.nextLine();
                String[] keyValue = inputData.split(":");

                Future<RecordMetadata> response = null;

                if (keyValue.length == 2) {
                    response = kafkaProducer.send(new ProducerRecord<>(topic, keyValue[0], keyValue[1]));
                } else if (keyValue.length == 1) {
                    response = kafkaProducer.send(new ProducerRecord<>(topic, keyValue[0]));
                } else {
                    log.error("Invalid input data: {}", inputData);
                }

                Optional.ofNullable(response).ifPresent(rsp -> {
                    try {
                        log.info("Message send {}", rsp.get());
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Error sending message ", e);
                        Thread.currentThread().interrupt();
                    }
                });
            } while (!inputData.equals("$exit"));

        }
    }
}
