package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.function.Supplier;

@RequiredArgsConstructor
public final class KafkaWriterImpl implements KafkaWriter {
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesProducer;

    @Override
    public void processing(Message message) {
        Message checkedMessage = ruleProcessor.processing(message, rulesProducer.get());
        if (checkedMessage.isFilterState()) {
            // sent
        }
    }
}
