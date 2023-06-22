package ru.mai.lessons.rpks.impl;
import lombok.Getter;
import lombok.NonNull;
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
public class KafkaReaderClass implements ru.mai.lessons.rpks.KafkaReader {
    private String topicReader;
    private String topicWriter;
    private String bootstrapServersReader;
    private String bootstrapServersWriter;
    private String groupId;
    private String autoOffset;

    @NonNull
    private Rule[] rules;
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaReaderClass(String topicReader,String topicWriter, String bootstrapServersReader,String bootstrapServersWriter, String groupId, String autoOffset, Rule[] rules){
        this.topicReader = topicReader;
        this.topicWriter = topicWriter;
        this.bootstrapServersReader = bootstrapServersReader;
        this.bootstrapServersWriter = bootstrapServersWriter;
        this.groupId = groupId;
        this.autoOffset = autoOffset;
        this.rules = rules;

        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersReader);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topicReader));
    }



    
    @Override
    public void processing() {
        RuleProcessorClass ruleProcessorClass = new RuleProcessorClass();
        KafkaWriterClass kafkaWriterClass = new KafkaWriterClass(topicWriter, bootstrapServersWriter);
        try {
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
        }
    }
}