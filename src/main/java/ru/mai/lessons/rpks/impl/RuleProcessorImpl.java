package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;

import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {

    @Override
    public Message processing(Message msg, Rule[] rules) {
        msg.setFilterState(false);
        String filterValue;
        String fieldValue;
        boolean state;
        for (Rule rule : Objects.requireNonNull(rules)) {
            filterValue = rule.getFilterValue();
            fieldValue = getFieldValue(msg.getValue(), rule.getFieldName());
            state = false;

            if (fieldValue == null || filterValue == null) {
                msg.setFilterState(false);
            }
            else {
                switch (rule.getFilterFunctionName()) {
                    case "equals" -> state = filterValue.equals(fieldValue);
                    case "not_equals" -> state = !fieldValue.isEmpty() && !filterValue.equals(fieldValue);
                    case "contains" -> state = fieldValue.contains(filterValue);
                    case "not_contains" -> state = !fieldValue.isEmpty() && !fieldValue.contains(filterValue);
                }
            }
            msg.setFilterState(state);

            if (!msg.isFilterState()) {
                return msg;
            }
        }

        return msg;
    }

    private String getFieldValue(String value, String fieldName) {
        ObjectMapper mapper = new ObjectMapper();

        Map map = null;
        try {
            map = mapper.readValue(value, Map.class);
        } catch (JsonProcessingException e) {
            return null;
        }
        return (map.get(fieldName) == null ? (null) : (map.get(fieldName).toString()));
    }

}