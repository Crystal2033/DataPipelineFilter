package ru.mai.lessons.rpks.scheduler;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.impl.DataBaseReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Getter
public class RulesUpdater implements Runnable{

    DataBaseReader reader;
    Object lock = new Object();
    List<Rule> rules;
    @Override
    public void run() {
        try {
            synchronized (lock) {
                if (reader.isConnected())
                    rules = new ArrayList<>(Arrays.stream(reader.readRulesFromDB()).toList());
                else {
                    // throw exception
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void initReader(Config config) throws SQLException {
        try {
            reader = new DataBaseReader(config);
        }
        catch (Exception e) {
            String str = e.toString();
            log.info(str);
        }

        if (!reader.connectToDataBase()) {
            // throw exception
        }
    }
}
