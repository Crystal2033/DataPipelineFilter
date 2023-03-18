package ru.mai.lessons.rpks.impl.repository;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
@Slf4j
public class RulesUpdaterThread implements Runnable{

    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;

    private final DataBaseReader dataBaseReader;

    private void insertNewRulesInMap(Rule[] rules){
        rulesConcurrentMap.clear();
        for(var rule : rules){
            List<Rule> myList;
            if((myList = rulesConcurrentMap.get(rule.getFieldName())) != null){
                myList.add(rule);
                continue;
            }
            myList = new ArrayList<>();
            myList.add(rule);
            rulesConcurrentMap.put(rule.getFieldName(), myList);
        }
        rulesConcurrentMap.entrySet().stream()
                .forEach(value -> log.debug(value.getValue().toString()));
    }
    @Override
    public void run() {
        Config config = ConfigFactory.load("application.conf");
        while(!Thread.currentThread().isInterrupted()){
            try {
                Rule[] rules = dataBaseReader.readRulesFromDB();
                insertNewRulesInMap(rules);
                log.info("Tick");
                Thread.sleep(config.getConfig("application")
                        .getLong("updateIntervalSec") * 100000);

            } catch (InterruptedException e) {
                log.error("Trouble with sleep of thread. " + e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
