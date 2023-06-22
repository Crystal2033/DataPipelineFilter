package ru.mai.lessons.rpks.impl;

import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class MyKafkaWriter implements KafkaWriter {
    String topic;
    String bootstrapServers;
    KafkaProducer<String, String> kafkaProducer;
    public MyKafkaWriter(String topic, String bootstrapServers){
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(props);
    }
    @Override
    public void processing(Message message) {
        try{
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message.getValue());
            kafkaProducer.send(producerRecord);
        }
        catch (Exception e){
            log.error("Create producer exception");
        }
    }
}
