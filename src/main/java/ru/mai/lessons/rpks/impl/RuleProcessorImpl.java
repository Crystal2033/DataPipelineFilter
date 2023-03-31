package ru.mai.lessons.rpks.impl;

import lombok.RequiredArgsConstructor;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiPredicate;

@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    private static final Map<String, BiPredicate<String, String>> functionNameAndItsBiPredicate = Map.of(
            "equals", (fieldValue, filterValue) -> filterValue.equals(fieldValue),
            "contains", (fieldValue, filterValue) -> filterValue.contains(fieldValue),
            "not_equals", (fieldValue, filterValue) -> !filterValue.equals(fieldValue),
            "not_contains", (fieldValue, filterValue) -> !filterValue.contains(fieldValue)
    );

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setFilterState(false);
            return message;
        }

        Arrays
                .stream(rules)
                .forEach(rule -> checkMessage(message, rule));
        return message;
    }

    private void checkMessage(Message message, Rule rule) {
        BiPredicate<String, String> fieldValueChecker
                = functionNameAndItsBiPredicate.get(rule.getFilterFunctionName());

        String fieldValue = getFieldValueFromJSON(message.getValue(), rule.getFieldName());
        String filterValue = rule.getFilterValue();

        boolean messageState = fieldValueChecker.test(fieldValue, filterValue);
        message.setFilterState(messageState);
    }

    private String getFieldValueFromJSON(String jsonString, String fieldName) {
        try {
            Object object = new JSONParser().parse(jsonString);
            JSONObject jsonObject = (JSONObject) object;
            return (String) jsonObject.get(fieldName);
        } catch (ParseException e) {
            return "";
        }
    }
}
