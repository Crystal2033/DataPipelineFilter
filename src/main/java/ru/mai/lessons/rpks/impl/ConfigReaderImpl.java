package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.ConfigReader;

public final class ConfigReaderImpl implements ConfigReader {
    private static final String DB_CONFIG_PATH = "dp";
    private static final String APPLICATION_CONFIG_PATH = "application";
    private static final String KAFKA_CONFIG_PATH = "application";

    @Override
    public Config loadConfig() {
         // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
        return ConfigFactory.load();
    }
}
