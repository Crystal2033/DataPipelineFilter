package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
@Data
public class MyKafkaWriter implements KafkaWriter {
    private Config config;
    @Override
    public void processing(Message message) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getConfig("kafka").getConfig("producer").getConfig("bootstrap").getString("servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test_topic_out", "1", message.getValue());
        producer.send(producerRecord, ((metadata, exception) -> {
            if (exception == null){
                log.info("All good");
            }else{
                log.error("error with" + message.getValue());
                log.error("error", exception);
            }
        }));

        producer.close();
    }
}
