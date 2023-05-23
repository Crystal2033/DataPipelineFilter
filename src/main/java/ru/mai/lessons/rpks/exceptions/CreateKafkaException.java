package ru.mai.lessons.rpks.exceptions;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CreateKafkaException extends Exception{
    private final String msg;
}
