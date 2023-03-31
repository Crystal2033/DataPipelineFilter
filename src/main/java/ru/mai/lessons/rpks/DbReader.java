package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;


public interface DbReader {
    public Rule[] readRulesFromDB(); // метод получает набор правил из БД PostgreSQL. Конфигурация для подключения из файла *.conf. Метод также должен проверять в заданное время с периодом изменения в БД и обновлять правила.

    interface KafkaWriter {
        void processing(Message message); // отправляет сообщения с filterState = true в выходной топик. Конфигурация берется из файла *.conf
    }
}
