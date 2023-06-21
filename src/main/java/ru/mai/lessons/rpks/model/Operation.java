package ru.mai.lessons.rpks.model;

public enum Operation {
    EQUALS("equals"), NOT_EQUALS("not_equals"), CONTAINS("contains"), NOT_CONTAINS("not_contains");
    public final String label;

    private Operation(String label) {
        this.label = label;
    }
}
