package ru.mai.lessons.rpks.impl;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

import com.google.gson.*;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Rule;
import ru.mai.lessons.rpks.model.Message;

public final class RuleProcessorImpl implements RuleProcessor {
    public enum RuleMode {
        EQUALS, NOT_EQUALS, CONTAINS, NOT_CONTAINS
    }

    private static final Map<RuleMode, BiPredicate<String, String>> fNameAndPredicate = Map.of(
            RuleMode.EQUALS, (fieldValue, filterValue) -> filterValue.equals(fieldValue),
            RuleMode.NOT_EQUALS, (fieldValue, filterValue) -> Optional.ofNullable(fieldValue).isPresent() &&
                    !fieldValue.isBlank() && !filterValue.equals(fieldValue),
            RuleMode.CONTAINS, (fieldValue, filterValue) -> Optional.ofNullable(fieldValue).isPresent() &&
                    fieldValue.contains(filterValue),
            RuleMode.NOT_CONTAINS, (fieldValue, filterValue) -> Optional.ofNullable(fieldValue).isPresent() &&
                    !fieldValue.isBlank() && !fieldValue.contains(filterValue)
    );

    @Override
    public Message processing(Message message, Rule[] rules) {
        if ((rules == null) || (rules.length == 0)) {
            message.setFilterState(false);
            return message;
        }

        for (Rule rule : rules)
            if (!setState(message, rule))
                return message;
        return message;
    }

    private Optional<String> getFieldFromJSON(String json, String fieldName) {
        try {
            JsonElement jsonElement = JsonParser.parseString(json).getAsJsonObject().get(fieldName);
            if (Optional.ofNullable(jsonElement).isEmpty() || jsonElement.isJsonNull())
                return Optional.empty();
            return Optional.of(jsonElement.getAsString());
        } catch (JsonParseException | IllegalStateException e) {
            return Optional.empty();
        }
    }

    private boolean setState(Message message, Rule rule) {
        BiPredicate<String, String> checker = fNameAndPredicate.get(RuleMode.valueOf(rule.getFilterFunctionName().toUpperCase()));
        boolean messageState = (getFieldFromJSON(message.getValue(), rule.getFieldName()).filter(
                s -> checker.test(s, rule.getFilterValue())).isPresent());
        message.setFilterState(messageState);
        return messageState;
    }
}