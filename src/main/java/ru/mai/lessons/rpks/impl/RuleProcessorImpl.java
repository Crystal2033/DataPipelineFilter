package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {

    @Override
    public Message processing(Message message, Rule[] rules) {
        return null;
    }
}
