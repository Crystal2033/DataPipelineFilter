package ru.mai.lessons.rpks.impl.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.exceptions.UndefinedOperationException;
import ru.mai.lessons.rpks.impl.kafka.dispatchers.DispatcherKafka;
import ru.mai.lessons.rpks.impl.kafka.dispatchers.FilteringDispatcher;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Getter
@Setter
@Builder
@AllArgsConstructor
public class KafkaReaderImpl implements KafkaReader {
    private final String topic;
    private final String groupId;
    private final String autoOffsetReset;
    private final String bootstrapServers;

    private final String exitWord;

    private final DispatcherKafka dispatcherKafka;
    private boolean isExit;

    @Override
    public void processing() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try (Closeable executor = executorService::shutdownNow) {
            KafkaConsumer<String, String> kafkaConsumer = initKafkaConsumer();

            kafkaConsumer.subscribe(Collections.singletonList(topic));

            listenAndDelegateFiltering(executorService, kafkaConsumer);
        } catch (IOException e) {
            log.error("There is a problem with binding closeable object to executor service.");
        }

    }

    private void listenAndDelegateFiltering(ExecutorService executorService, KafkaConsumer<String, String> kafkaConsumer) {
        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    if (consumerRecord.value().equals(exitWord)) {
                        if (dispatcherKafka instanceof FilteringDispatcher filteringDispatcher) {
                            filteringDispatcher.closeReadingThread();
                            log.info("Closing thread");
                        }
                        isExit = true;
                    } else {
                        log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
                        executorService.execute(() -> sendToFilter(consumerRecord.value()));
                    }
                }
            }
        }
    }

    private KafkaConsumer<String, String> initKafkaConsumer() {
        log.info("Start reading kafka topic {}", topic);
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "groupId: " + groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    private void sendToFilter(String msg) {
        try {
            log.info("Before action with message {}", msg);
            dispatcherKafka.actionWithMessage(msg);
            log.info("After action with message {}", msg);
        } catch (UndefinedOperationException ex) {
            log.error("The operation {} not found.", ex.getOperation());
        }

    }
}
