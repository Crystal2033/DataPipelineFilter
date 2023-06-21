package ru.mai.lessons.rpks.impl;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafkaReaderClass implements ru.mai.lessons.rpks.KafkaReader {
    private final String topicReader;
    private final String topicWriter;
    private final String bootstrapServersReader;
    private final String bootstrapServersWriter;
    @NonNull
    private Rule[] rules;


    @Override
    public void processing() {
        RuleProcessorClass ruleProcessorClass = new RuleProcessorClass();
        KafkaWriterClass kafkaWriterClass = new KafkaWriterClass(topicWriter, bootstrapServersWriter);
        KafkaConsumer<String, String> kafkaConsumer = null;

        try {
            kafkaConsumer = createConsumer();
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    Message message = new Message(consumerRecord.value(), true);
                    message = ruleProcessorClass.processing(message, rules);
                    if (message.isFilterState()) {
                        kafkaWriterClass.processing(message);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception while creating consumer", e);
        } finally {
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        }
    }

    public KafkaConsumer<String, String> createConsumer() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersReader);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topicReader));
        return consumer;
    }

}