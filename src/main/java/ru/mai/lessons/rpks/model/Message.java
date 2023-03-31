package ru.mai.lessons.rpks.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {
    private String value; // сообщение из Kafka в формате JSON

    public Message(String value) {
        this.value = value;
        this.filterState = false;
    }

    private boolean filterState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}
