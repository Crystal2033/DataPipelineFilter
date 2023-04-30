package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiPredicate;

@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private static final Map<String, BiPredicate<String, String>> functionNameAndItsBiPredicate = Map.of(
            "equals", (fieldValue, filterValue) -> filterValue.equals(fieldValue),
            "contains", (fieldValue, filterValue) -> fieldValue != null && fieldValue.contains(filterValue),
            "not_equals", (fieldValue, filterValue) -> fieldValue != null && !fieldValue.equals("") && !filterValue.equals(fieldValue),
            "not_contains", (fieldValue, filterValue) -> fieldValue != null && !fieldValue.equals("") && !fieldValue.contains(filterValue)
    );

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setFilterState(false);
            return message;
        }

        for (Rule rule : rules) {
            if (!setMessageState(message, rule)) {
                return message;
            }
        }
        return message;
    }

    private boolean setMessageState(Message message, Rule rule) {
        BiPredicate<String, String> fieldValueChecker = functionNameAndItsBiPredicate.get(rule.getFilterFunctionName());

        String fieldValue = getFieldValueFromJSON(message.getValue(), rule.getFieldName());
        String filterValue = rule.getFilterValue();

        boolean messageState = fieldValueChecker.test(fieldValue, filterValue);
        message.setFilterState(messageState);

        return messageState;
    }

    private String getFieldValueFromJSON(String jsonString, String fieldName) {
        try {
            ObjectNode node = new ObjectMapper().readValue(jsonString, ObjectNode.class);
            if (node.has(fieldName)) {
                return node.get(fieldName).asText();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}
