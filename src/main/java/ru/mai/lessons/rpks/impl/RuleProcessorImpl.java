package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {

    @Override
    public Message processing(Message message, Rule[] rules) {
        message.setFilterState(false);

        for (Rule rule : Objects.requireNonNull(rules)) {
            processingMessage(message, rule);
            if (!message.isFilterState()) {
                return message;
            }
        }

        return message;
    }

    private void processingMessage(Message message, Rule rule) {
        String filterValue = rule.getFilterValue();
        String fieldValue = getFieldValue(message.getValue(), rule.getFieldName());

        if (fieldValue == null || filterValue == null) {
            message.setFilterState(false);
            return;
        }

        boolean state;

        switch (rule.getFilterFunctionName()) {
            case "equals" -> state = filterValue.equals(fieldValue);
            case "contains" -> state = fieldValue.contains(filterValue);
            case "not_equals" -> state = !fieldValue.isEmpty() && !filterValue.equals(fieldValue);
            case "not_contains" -> state = !fieldValue.isEmpty() && !fieldValue.contains(filterValue);
            default -> state = false;

        }

        message.setFilterState(state);
    }

    private String getFieldValue(String value, String fieldName) {
        ObjectMapper mapper;

        mapper = new ObjectMapper();

        Map<Object, Object> map = null;
        try {
            map = mapper.readValue(value, Map.class);
        } catch (JsonProcessingException e) {
            return null;
        }
        return (map.get(fieldName) == null ? (null) : (map.get(fieldName).toString()));
    }

}