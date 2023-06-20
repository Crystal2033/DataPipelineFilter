package ru.mai.lessons.rpks.impl;

import ru.mai.lessons.rpks.model.Rule;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@RequiredArgsConstructor
@Slf4j
public class RulesUpdaterImpl implements Runnable {
    private final DbReaderImpl dbReader;
    private final Long updateIntervalSec;
    private final ConcurrentLinkedQueue<Rule> rulesConcurrent;
    @Override
    public void run() {
        Rule[] rules = dbReader.readRulesFromDB();
        rulesConcurrent.clear();
        rulesConcurrent.addAll(List.of(rules));
    }
}
