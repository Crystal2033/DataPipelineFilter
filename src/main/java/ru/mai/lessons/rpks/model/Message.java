package ru.mai.lessons.rpks.model;

import lombok.*;

@Data
@Setter
@RequiredArgsConstructor
public class Message {
    final private String value; // сообщение из Kafka в формате JSON

    @Getter @Setter private boolean filterState; // true - удовлетворены условиях всех правил (Rule), false - хотя бы одно условие не прошло проверку.
    public boolean getFilterState(){
        return(filterState);
    }

    public void setFilterState(boolean b) {
        filterState = b;
    }
}
