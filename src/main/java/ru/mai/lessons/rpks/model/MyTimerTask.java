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
    //    ArrayList<String> list = new ArrayList<>();
    Db db;
    DSLContext context;
    Rule[] rules;
    boolean isReady = false;
//    public Rule[] getRules(){
//        return this.rules;
//    }
    public MyTimerTask(DSLContext context, Db db, Rule[] rules) {
        this.context = context;
        this.db = db;
        this.rules = rules;

    }

    @Override
    public void run() {
        System.out.println("TimerTask начал свое выполнение в:" + new Date());
        isReady = false;
        completeTask();
        System.out.println("TimerTask закончил свое выполнение в:" + new Date());
    }

    void completeTask() {
        try {
            // допустим, выполнение займет 20 секунд
            Thread.sleep(5000);
           synchronized (rules) {
               rules = db.readRulesFromDB(context);
               log.info("RULES UPDATED {}", rules.length);
               isReady = true;
           }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean getIsReady() {
        return isReady;
    }
}
