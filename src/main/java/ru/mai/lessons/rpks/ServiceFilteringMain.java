package ru.mai.lessons.rpks;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.impl.ConfigurationReaderClass;
import ru.mai.lessons.rpks.impl.ServiceFilteringClass;

@Slf4j
public class ServiceFilteringMain {
    public static void main(String[] args) {
        log.debug("Starting service filtering");
        ConfigReader configReader = new ConfigurationReaderClass();
        Service service = new ServiceFilteringClass();
        service.start(configReader.loadConfig());
    }
}