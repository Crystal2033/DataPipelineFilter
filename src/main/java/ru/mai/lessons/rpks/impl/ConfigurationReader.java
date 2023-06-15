package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ConfigReader;
@Slf4j
public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        Config config= ConfigFactory.load();
        log.debug("starting");
        log.debug(config.getConfig("kafka").toString());
        return config;
         // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
