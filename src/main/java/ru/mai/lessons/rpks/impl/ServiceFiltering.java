package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.*;

public class ServiceFiltering implements Service {
    @Override
    public void start(Config config) {
        KafkaReader kafkaReader = new KafkaReadeImpl(config);
        kafkaReader.processing();
    }
}
