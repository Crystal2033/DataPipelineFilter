package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.ConfigReader;

import java.io.File;
import java.net.URL;

@Slf4j
public class ConfigurationReader implements ConfigReader {
    @Override
    public Config loadConfig() {
        ClassLoader classLoader = getClass().getClassLoader();
        URL urlResources = classLoader.getResource("application.conf") ;
        if (urlResources == null){
            log.error("file not found");
            return null;
        }

        File fileConf = new File(urlResources.getPath());
        Config config = ConfigFactory.parseFile(fileConf);
        if(config.isEmpty()){
            log.error("config is empty");
            return null;
        }

        log.info(config.toString());
        return config; // написать код загрузки конфигураций сервиса фильтраций из файла *.conf
    }
}
