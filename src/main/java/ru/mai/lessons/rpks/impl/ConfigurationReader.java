package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.ConfigReader;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        Config config = loadConfig();
        return config; // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
