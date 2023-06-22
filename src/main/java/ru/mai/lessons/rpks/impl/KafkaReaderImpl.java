package ru.mai.lessons.rpks.impl;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Builder
@Slf4j
public class KafkaReaderImpl implements KafkaReader {

    String bootstrapServers;
    String bootstrapServersOut;
    String groupId;
    String autoOffsetReset;
    String topic;
    String topicOut;
    private final Supplier<Rule[]> supplier;//Rule[] rules;
    Rule[] rules;


    @Override
    public void processing() {// запускает KafkaConsumer в бесконечном цикле и читает сообщения. Внутри метода происходит обработка сообщений по правилам и отправка сообщений в Kafka выходной топик. Конфигурация для консьюмера из файла *.conf
        Properties props = new Properties();
        log.debug("props");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        log.debug("props");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        log.debug("props");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        log.debug("props");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        log.debug("props");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        log.debug("props");
        RuleProcessorImpl ruleProcessor = new RuleProcessorImpl();
        KafkaWriterImpl kafkaWriter = new KafkaWriterImpl(topicOut, bootstrapServersOut);
        log.debug("Start reading kafka topic {}", topic);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));
        ExecutorService executor;
        executor = Executors.newSingleThreadExecutor();

        executor.submit(() ->{
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords){
                    log.debug("Check message");
                    rules = supplier.get();
                    Message message = new Message(consumerRecord.value());
                    if ((ruleProcessor.processing(message, rules)).getFilterState()){
                        kafkaWriter.processing(message);
                    }

                }
            }
        });
        log.debug("Read is done!");
    }
}
