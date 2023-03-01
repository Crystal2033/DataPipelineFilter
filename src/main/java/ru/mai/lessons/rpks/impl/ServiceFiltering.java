package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ServiceFiltering implements Service {

    public String kafkaBootsTrap;

    @Override
    public void start(Config config) {
        kafkaBootsTrap = config.getString("kafka.bootstrap.servers");
        // написать код реализации сервиса фильтрации
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootsTrap,
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
        try {
            kafkaProducer.send(new ProducerRecord<>("test_topic_out","{\"name\":\"alexander\", \"age\":18, \"sex\":\"M\"}")).get();
            kafkaProducer.flush();
            kafkaProducer.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
