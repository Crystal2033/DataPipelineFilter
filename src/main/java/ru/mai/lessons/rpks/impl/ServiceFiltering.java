package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class ServiceFiltering implements Service {
    Rule[] rules;
    DbReader db;

    TimerTask task = new TimerTask() {
        @Override
        public void run() {
            rules = db.readRulesFromDB();
        }
    };

    @Override
    public void start(Config config) {
        db = new DbReaderImpl(config);
        rules = db.readRulesFromDB();

        Timer time = new Timer();
        time.schedule(task, 0, config.getLong("application.updateIntervalSec") * 1000);


    }
}
