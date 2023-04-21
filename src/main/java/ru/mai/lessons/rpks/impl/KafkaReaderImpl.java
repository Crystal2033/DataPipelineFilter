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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class KafkaReaderImpl {

    private final String topic;
    private final String topicOut;
    private final String bootstrapServers;
    @NonNull
    Rule[] rules;
    private boolean isExit;
    ConcurrentLinkedQueue<Message> queue;
//    KafkaWriterImpl kafkaWriter;
//    KafkaQueueWriter kafkaQueueWriter;

    public void processing() {
        log.info("Start reading kafka topic {}", topic);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );

        kafkaConsumer.subscribe(Collections.singletonList(topic));


        try (kafkaConsumer) {
            while (!isExit) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    if (consumerRecord.value().equals("$exit")) {
                        isExit = true;
                    } else {
                        log.info("Message from Kafka topic {} : {}", consumerRecord.topic(), consumerRecord.value());

                        log.info(String.valueOf(consumerRecord));
                        queue = new ConcurrentLinkedQueue<>();
//                        queue.add(consumerRecord.value());
                        Message msg = new Message(consumerRecord.value(), true);
                        RuleProcessorImpl ruleProcessor = new RuleProcessorImpl();
                        queue = new ConcurrentLinkedQueue<>();
                        queue.add(ruleProcessor.processing(msg, rules));
                        System.out.println("_______________________"+queue);
                        log.info("Start write message in kafka out topic {}", topicOut);
                        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                                Map.of(
                                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                                        ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                                ),
                                new StringSerializer(),
                                new StringSerializer()
                        )) {
                            if (!queue.isEmpty()) {
                                Message queueElement = queue.peek();
                                log.info("Queue element {}", queueElement);
                                queue.remove();
                                Future<RecordMetadata> response = null;

                                if (queueElement.getFilterState() == true) {
                                    if (Objects.equals(queueElement.getValue(), "$exit")) {
                                        isExit = true;
                                        break;
                                    }
                                    response = kafkaProducer.send(new ProducerRecord<>(topicOut, queueElement.getValue()));
                                    //                    } else if (keyValue.length == 1) {
                                    //                        response = kafkaProducer.send(new ProducerRecord<>(topic, keyValue[0]));


                                    Optional.ofNullable(response).ifPresent(rsp -> {
                                        try {
                                            log.info("Message send to out{}", rsp.get());
                                        } catch (InterruptedException | ExecutionException e) {
                                            log.error("Error sending message ", e);
                                            Thread.currentThread().interrupt();
                                        }
                                    });
                                }
                            }
                        }catch (Exception e) {
                            e.printStackTrace();
                        }
//                        for (int i = 0; i< rules.length; i++){
//                            if (Objects.equals(rules[i].getFilterFunctionName(), "equals")){
//                                Message msg = new Message(consumerRecord.value(), true);
//                                if (!Objects.equals(consumerRecord.value(), rules[i].getFilterValue())){
//                                    msg.setFilterState(false);
//                                }
//                            }
//                            if (Objects.equals(rules[i].getFilterFunctionName(), "not_equals")){
//                                Message msg = new Message(consumerRecord.value(), true);
//                                if (Objects.equals(consumerRecord.value(), rules[i].getFilterValue())){
//                                    msg.setFilterState(false);
//                                }
//                            }
//                            if (Objects.equals(rules[i].getFilterFunctionName(), "contains")){
//                                Message msg = new Message(consumerRecord.value(), true);
//                                if (consumerRecord.value(), rules[i].getFilterValue())){
//                                    msg.setFilterState(false);
//                                }
//                            }
//                        }
                    }
                }
            }
            log.info("Read is done!");
        }
    }
}
