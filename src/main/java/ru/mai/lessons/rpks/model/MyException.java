package ru.mai.lessons.rpks.model;

public class MyException
        extends RuntimeException {
    public MyException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
