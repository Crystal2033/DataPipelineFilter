package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@RequiredArgsConstructor
@Slf4j
public class RulesUpdaterThread implements Runnable {

    private final DBReaderImpl dbReader;
    private final Long updateIntervalSec;
    private final ConcurrentLinkedQueue<Rule> rulesConcurrent;
    private boolean isExit = false;

    public void stopReading() {
        isExit = true;
    }

    @Override
    public void run() {
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                isExit = false;
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        });
            Rule[] rules = dbReader.readRulesFromDB();
            rulesConcurrent.clear();
            rulesConcurrent.addAll(List.of(rules));
    }
}
