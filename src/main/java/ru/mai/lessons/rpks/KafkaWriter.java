package ru.mai.lessons.rpks;

public interface KafkaWriter {
    public void processing(); // отправляет сообщения с filterState = true в выходной топик. Конфигурация берется из файла *.conf
}
