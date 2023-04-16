package ru.mai.lessons.rpks.impl;

import java.util.Map;
import java.util.function.BiPredicate;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.model.Message;

public final class RuleProcessorImpl implements RuleProcessor {
    private static final Map<String, BiPredicate<String, String>> fNameAndPredicate = Map.of(
            "equals", (fieldValue, filterValue) -> filterValue.equals(fieldValue),
            "not_equals", (fieldValue, filterValue) -> fieldValue != null && !fieldValue.equals("") && !filterValue.equals(fieldValue),
            "contains", (fieldValue, filterValue) -> fieldValue != null && fieldValue.contains(filterValue),
            "not_contains", (fieldValue, filterValue) -> fieldValue != null && !fieldValue.equals("") && !fieldValue.contains(filterValue)
    );

    @Override
    public Message processing(Message message, Rule[] rules) {
        if ((rules == null) || (rules.length == 0)) {
            message.setFilterState(false);
            return message;
        }

        for (Rule rule : rules) {
            if (!setState(message, rule))
                break;
        }
        return message;
    }

    private String getFieldFromJSON(String json, String fieldName) {
        try {
            JSONObject jsonObject = (JSONObject) new JSONParser().parse(json);
            return ((jsonObject.get(fieldName) != null) ? jsonObject.get(fieldName).toString() : "");
        } catch (ParseException e) {
            return "";
        }
    }

    private boolean setState(Message message, Rule rule) {
        BiPredicate<String, String> checker = fNameAndPredicate.get(rule.getFilterFunctionName());
        boolean messageState = checker.test(getFieldFromJSON(message.getValue(), rule.getFieldName()), rule.getFilterValue());
        message.setFilterState(messageState);
        return messageState;
    }
}