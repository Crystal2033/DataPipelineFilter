package ru.mai.lessons.rpks;

import lombok.Builder;

public interface KafkaReader {
    void processing(); // запускает KafkaConsumer в бесконечном цикле и читает сообщения. Внутри метода происходит обработка сообщений по правилам и отправка сообщений в Kafka выходной топик. Конфигурация для консьюмера из файла *.conf
}
