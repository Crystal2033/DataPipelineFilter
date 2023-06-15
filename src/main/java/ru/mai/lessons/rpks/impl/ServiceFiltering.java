package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.scheduler.RulesScheduler;

public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        RulesScheduler rulesScheduler = new RulesScheduler();
        rulesScheduler.runScheduler(5);
        // написать код реализации сервиса фильтрации
    }
}
