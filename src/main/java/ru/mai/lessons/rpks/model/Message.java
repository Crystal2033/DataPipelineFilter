package ru.mai.lessons.rpks.model;

import lombok.Data;

@Data
public class Message {
    private String value; // сообщение из Kafka в формате JSON

    private boolean filterState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.

    public Message(String rec) {
        this.value = rec;
        this.filterState = false;
    }
}
