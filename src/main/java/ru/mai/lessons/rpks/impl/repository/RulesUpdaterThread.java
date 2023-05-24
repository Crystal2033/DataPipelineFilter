package ru.mai.lessons.rpks.impl.repository;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Slf4j
@Getter
public class RulesUpdaterThread implements Runnable {

    private final ConcurrentHashMap<String, List<Rule>> rulesConcurrentMap;

    private final DataBaseReader dataBaseReader;

    private void insertNewRulesInMap(Rule[] rules) {
        rulesConcurrentMap.clear();
        for (var rule : rules) {
            List<Rule> myList;
            if ((myList = rulesConcurrentMap.get(rule.getFieldName())) != null) {
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
        try {
            Rule[] rules = dataBaseReader.readRulesFromDB();
            insertNewRulesInMap(rules);
            log.info("New rules have been inserted.");
            log.info("Is connected to database: {}", dataBaseReader.isConnectedToDataBase());

        }
        catch (SQLException e) {
            log.error("Bad connection to database!", e);
        }

    }
}
