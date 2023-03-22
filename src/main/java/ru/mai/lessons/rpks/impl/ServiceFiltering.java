package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.Service;

@Slf4j
public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        // написать код реализации сервиса фильтрации
        log.info(config.toString());
        //Этот метод будет выполняться в побочном потоке
        Thread thread1 = new Thread(() -> {
            MyKafkaReader kafkaReader = new MyKafkaReader();
            kafkaReader.setConfig(config);
            kafkaReader.setSubscribeString("test_topic_in");
            log.info("some2");
            kafkaReader.processing();
        });

        thread1.start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("exit from ServiceFiltering.start");
    }
}
