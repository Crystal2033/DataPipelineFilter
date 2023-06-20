package ru.mai.lessons.rpks.impl;

import lombok.Data;

import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.concurrent.ConcurrentLinkedQueue;

@Data
public class MessageHandler {

    private final KafkaWriterImpl producer;
    private final RuleProcessorImpl ruleProcessor;
    private final ConcurrentLinkedQueue<Rule> rules;
    private final RulesUpdaterImpl rulesUpdater;

    void processMessage(Message msg) {
        Rule[] ruleArr = new Rule[rules.size()];
        ruleProcessor.processing(msg, rules.toArray(ruleArr));
        if (msg.isFilterState())
            producer.processing(msg);
    }
}
