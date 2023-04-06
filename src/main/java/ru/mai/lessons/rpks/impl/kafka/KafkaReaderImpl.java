package ru.mai.lessons.rpks.impl.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.impl.constants.MainNames;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

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
    private boolean isExit;
    @Override
    public void processing() {
        log.info("Start reading kafka topic {}", topic);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "groupId: " + groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        kafkaConsumer.subscribe(Collections.singletonList(topic));

        try (kafkaConsumer) {
            Config config = ConfigFactory.load(MainNames.CONF_PATH).getConfig("kafka");
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    if (consumerRecord.value().equals(config.getString("exit.string"))) {
                        isExit = true;
                    } else {
                        //TODO: work with data.
                        log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());
                    }
                }

            }
        }
    }
}
