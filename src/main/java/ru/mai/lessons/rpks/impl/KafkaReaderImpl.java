package ru.mai.lessons.rpks.impl;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;

@RequiredArgsConstructor
public final class KafkaReaderImpl implements KafkaReader {
    private final KafkaWriter kafkaWriter;

    @Override
    public void processing() {

    }
}
