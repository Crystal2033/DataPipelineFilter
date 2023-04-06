package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.ConfigReader;
import ru.mai.lessons.rpks.impl.constants.MainNames;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.load(MainNames.CONF_PATH);
    }
}
