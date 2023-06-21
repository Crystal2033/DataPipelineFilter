package ru.mai.lessons.rpks.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;

@Data
@Builder
@AllArgsConstructor
@Setter
public class Message {
    private String value; // сообщение из Kafka в формате JSON

    private boolean filterState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
}