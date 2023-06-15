package ru.mai.lessons.rpks.impl;

import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

public class DataBaseReader implements DbReader {

    @Override
    public Rule[] readRulesFromDB() {
        return new Rule[0];
    }
}
