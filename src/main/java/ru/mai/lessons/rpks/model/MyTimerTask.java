package ru.mai.lessons.rpks.model;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.False;
import ru.mai.lessons.rpks.impl.Db;

import java.util.ArrayList;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
@Slf4j
@Getter
public class MyTimerTask extends TimerTask {
    Db db;
    DSLContext context;
    Rule[] rules;
    boolean isReady = false;

    public MyTimerTask(DSLContext context, Db db, Rule[] rules) {
        this.context = context;
        this.db = db;
        this.rules = rules;

    }

    @Override
    public void run() {
        log.info("TimerTask начал свое выполнение в:" + new Date());
        isReady = false;
        completeTask();
        log.info("TimerTask закончил свое выполнение в:" + new Date());
    }

    synchronized void completeTask() {
            rules = db.readRulesFromDB(context);
            log.info("RULES UPDATED {}", rules.length);
            isReady = true;
    }

    public boolean getIsReady() {
        return isReady;
    }
}
