package ru.mai.lessons.rpks.exceptions;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class UndefinedOperationException extends Exception{
    private final String msg;
}
