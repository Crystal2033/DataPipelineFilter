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
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class MyKafkaReader implements KafkaReader {
    private final String topicReader;
    private final String topicWriter;
    private final String bootstrapServersReader;
    private final String bootstrapServersWriter;
    @NonNull
    private Rule[] rules;
    @Override
    public void processing() {
        MyRuleProcessor myRuleProcessor = new MyRuleProcessor();
        MyKafkaWriter myKafkaWriter = new MyKafkaWriter(topicWriter, bootstrapServersWriter);

        try (KafkaConsumer<String, String> kafkaConsumer = createConsumer()) {

            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    Message message = new Message(consumerRecord.value(), true);
                    message = myRuleProcessor.processing(message, rules);
                    if (message.isFilterState()) {
                        myKafkaWriter.processing(message);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Create consumer exception", e);
        }
    }
    public KafkaConsumer<String, String> createConsumer(){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersReader);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topicReader));

        return consumer;
    }

}
