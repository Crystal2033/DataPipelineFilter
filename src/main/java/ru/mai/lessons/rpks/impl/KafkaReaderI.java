package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
@Slf4j
public class KafkaReaderI implements KafkaReader {
    private final KafkaConsumer<String, String> consumer;
    private final KafkaWriter writer;
    private final RuleProcessorI checkerRules;
    private final Supplier<Rule[]> supplier;
    private Rule [] rules;
    public KafkaReaderI(Config conf, KafkaWriter writer, RuleProcessorI checker, Supplier<Rule[]> supplier){
        this.writer = writer;
        this.supplier = supplier;
        Properties props = new Properties();
        String consumerStr = "consumer";
        props.put("bootstrap.servers", conf.getConfig(consumerStr).getString("bootstrap.servers"));
        props.put("group.id", conf.getConfig(consumerStr).getString("group.id"));
        props.put("auto.offset.reset", conf.getConfig(consumerStr).getString("auto.offset.reset"));
        consumer = new KafkaConsumer<>(props,  new StringDeserializer(),
                new StringDeserializer());
        consumer.subscribe(Collections.singletonList(conf.getConfig(consumerStr).getString("topic")));
        checkerRules = checker;
    }
    @Override
    public void processing() {
        ExecutorService executor;
        executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                rules = supplier.get();
                for (ConsumerRecord<String, String> MyRecord : records) {
                    Message msg = checkerRules.processing(new Message(MyRecord.value()), rules);
                    writer.processing(msg);
                }
            }
        });
    }
}
