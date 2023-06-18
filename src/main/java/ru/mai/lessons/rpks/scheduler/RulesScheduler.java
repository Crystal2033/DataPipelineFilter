package ru.mai.lessons.rpks.scheduler;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RulesScheduler {
    private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private RulesUpdater checker = new RulesUpdater();

    public void runScheduler(Config config) throws SQLException {
        checker.rules = new ArrayList<>();
        checker.setDBParams(config);
        checker.initReader();
        service.scheduleAtFixedRate(checker, 0,
                config.getInt("application.updateIntervalSec"), TimeUnit.SECONDS);
    }

    public List<Rule> getRules() {
        return checker.getRules();
    }
}
