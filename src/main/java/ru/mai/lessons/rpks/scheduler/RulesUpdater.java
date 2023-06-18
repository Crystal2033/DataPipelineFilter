package ru.mai.lessons.rpks.scheduler;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.Synchronized;
import ru.mai.lessons.rpks.impl.DataBaseReader;
import ru.mai.lessons.rpks.model.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
public class RulesUpdater implements Runnable{

    DataBaseReader reader = new DataBaseReader();
    List<Rule> rules;
    @Override
    public void run() {
        try {
            synchronized(rules) {
                if (reader.isConnected())
                rules = new ArrayList<>(Arrays.stream(reader.readRulesFromDB()).toList());
            else {
                // throw exception
            }
            System.out.println();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setDBParams(Config config) {
        reader.setUrl(config.getString("db.jdbcUrl"));
        reader.setPass(config.getString("db.password"));
        reader.setUser(config.getString("db.user"));
        reader.setDriver(config.getString("db.driver"));
    }

    public void initReader() throws SQLException {
        reader.setHikariParams();
        if (!reader.connectToDataBase()) {
            // throw exception
        }
    }
}
