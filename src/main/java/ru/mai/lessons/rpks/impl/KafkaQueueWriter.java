package ru.mai.lessons.rpks.impl;

import lombok.NonNull;
import lombok.Setter;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@RequiredArgsConstructor
@Setter

public class KafkaQueueWriter implements KafkaWriter {
    private final String topic;
    private final String bootstrapServers;
    @NonNull
    ConcurrentLinkedQueue<Message> queue; //all messages
    boolean isExit = false;

    public void processing() {
        log.info("Start queue write message in kafka topic {}", topic);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        )) {
            while (!isExit) {
                if (!queue.isEmpty()) {
                    Message queueElement = queue.peek();
                    queue.remove();
                    Future<RecordMetadata> response = null;

                    if (queueElement.getFilterState()) {
                        if (Objects.equals(queueElement.getValue(), "$exit")) {
                            isExit = true;
                            break;
                        }
                        response = kafkaProducer.send(new ProducerRecord<>(topic, queueElement.getValue()));
                    }
//                    else {
//                        log.error("Invalid input data: {}", queueElement);
//                    }

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
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

