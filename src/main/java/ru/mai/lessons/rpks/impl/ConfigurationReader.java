package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import ru.mai.lessons.rpks.ConfigReader;

@Slf4j
public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig()
    {
        return ConfigFactory.load(); // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
