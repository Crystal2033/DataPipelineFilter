package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;


@Slf4j
public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        //Этот метод будет выполняться в побочном потоке
        MyKafkaReader kafkaReader = new MyKafkaReader(config);
        kafkaReader.processing();
        log.info("exit from ServiceFiltering.start");
    }
}
