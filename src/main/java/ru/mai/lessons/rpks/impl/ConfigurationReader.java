package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.ConfigReader;
import com.typesafe.config.ConfigFactory;

public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        return ConfigFactory.load(); // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
