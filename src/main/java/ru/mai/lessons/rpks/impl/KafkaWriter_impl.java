package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
public class KafkaWriter_impl implements KafkaWriter {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaWriter_impl(Config appConfig) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getString("kafka.producer.bootstrap.servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        topic = appConfig.getString("kafka.producer.topic");
        producer = new KafkaProducer<>(config);
    }

    @Override
    public void processing(Message message) {
        producer.send(new ProducerRecord<>(topic, message.getValue()), (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending message to Kafka: {}", exception.getMessage());
            } else {
                log.info("Message sent to Kafka successfully");
            }
        });
    }
}
