package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.function.Supplier;

@Builder
public final class KafkaWriterImpl implements DbReader.KafkaWriter {
    private final RuleProcessor ruleProcessor;
    private final Supplier<Rule[]> rulesGetter;
    private final String topic;
    private final String bootstrapServers;

    @Override
    public void processing(Message message) {
        Message checkedMessage = ruleProcessor.processing(message, rulesGetter.get());
        if (checkedMessage.isFilterState()) {
            send(message);
        }
    }

    private void send(Message message) {

    }
}
