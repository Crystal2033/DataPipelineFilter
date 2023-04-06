package ru.mai.lessons.rpks.impl.repository;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.impl.constants.MainNames;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Slf4j
public class RulesUpdaterThread implements Runnable{

    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;

    private final DataBaseReader dataBaseReader;

    private boolean isExit = false;

    public void stopReadingDataBase(){
        isExit = true;
    }
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
        rulesConcurrentMap.forEach((key, value1) -> log.debug(value1.toString()));
    }
    @Override
    public void run() {
        Config config = ConfigFactory.load(MainNames.CONF_PATH);
        while(!isExit){
            try {
                Rule[] rules = dataBaseReader.readRulesFromDB();
                insertNewRulesInMap(rules);
                log.info("Tick");
                Thread.sleep(config.getConfig("application")
                        .getLong("updateIntervalSec") * 1000);

            } catch (InterruptedException e) {
                log.error("Trouble with sleep of thread. " + e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
