package ru.mai.lessons.rpks;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mai.lessons.rpks.model.Rule;

public interface RuleApplier {
    boolean apply(ConsumerRecord<String, String> consumerRecord, Rule[] rules);
}
