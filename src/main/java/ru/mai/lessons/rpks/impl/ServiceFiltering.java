package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.scheduler.RulesScheduler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ServiceFiltering implements Service {

    List<Rule> rules = new ArrayList<>();
    private Config config;
    private ConfigurationReader reader = new ConfigurationReader();
    private DataBaseReader dataBaseReader = new DataBaseReader();
    private KafkaReaderRealization kafkaReader = new KafkaReaderRealization();
    @Override
    public void start(Config config) {
        config = reader.loadConfig();

        // написать код реализации сервиса фильтрации
        kafkaReader.setConfig(config);
        kafkaReader.setRuleList(rules);
        kafkaReader.processing();

    }


}
