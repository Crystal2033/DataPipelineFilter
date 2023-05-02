package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

public class KafkaWriterI implements KafkaWriter {
    private Properties props = new Properties();
    KafkaProducer<String, String> producer;
    String topic;

    public KafkaWriterI(Config conf){
        props.put("bootstrap.servers", conf.getConfig("kafka").getConfig("producer").getString("bootstrap.servers"));
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        topic = conf.getConfig("kafka").getConfig("producer").getString("topic");
    }
    @Override
    public void processing(Message message) {
        if (message.isFilterState())
            producer.send(new ProducerRecord<>(topic, message.getValue()));
    }
}
