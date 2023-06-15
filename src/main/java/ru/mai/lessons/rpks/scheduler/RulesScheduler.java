package ru.mai.lessons.rpks.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RulesScheduler {
    private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private RulesChecker checker = new RulesChecker();

    public void runScheduler(int timeout){
        service.scheduleAtFixedRate(checker, 0, timeout, TimeUnit.SECONDS);
    }
}
