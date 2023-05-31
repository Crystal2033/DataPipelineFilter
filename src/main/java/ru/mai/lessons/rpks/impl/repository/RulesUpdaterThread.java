package ru.mai.lessons.rpks.impl.repository;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Getter
public class RulesUpdaterThread implements Runnable {

    private final DataBaseReader dataBaseReader;
    private List<Rule> rules;

    @Override
    public void run() {
        rules = new ArrayList<>(List.of(dataBaseReader.readRulesFromDB()));
    }
}
