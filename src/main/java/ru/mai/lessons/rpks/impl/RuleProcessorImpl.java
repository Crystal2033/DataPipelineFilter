package ru.mai.lessons.rpks.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

@Slf4j
public final class RuleProcessorImpl implements RuleProcessor {

    public enum FilterFunctions {
        EQUALS_OPERATION("EQUALS"),
        NOT_EQUALS("NOT_EQUALS"),
        CONTAINS("CONTAINS"),
        NOT_CONTAINS("NOT_CONTAINS");

        private final String func;

        FilterFunctions(String func) {
            this.func = func;
        }

        @Override
        public String toString() {
            return func;
        }
    }

    private final Map<String, BiPredicate<String, String>> filterFunctionsProcesses = Map.of(
            FilterFunctions.EQUALS_OPERATION.toString(), String::equals,
            FilterFunctions.NOT_EQUALS.toString(), (messageValue, filterValue) -> !messageValue.equals(filterValue),
            FilterFunctions.CONTAINS.toString(), String::contains,
            FilterFunctions.NOT_CONTAINS.toString(), (messageValue, filterValue) -> !messageValue.contains(filterValue));


    private Message processMessage(Message message, JsonObject jsonMessage, List<Rule> groupedRules) {
        for (Rule rule : groupedRules) {
            if (jsonMessage.has(rule.getFieldName())) {
                String curMessageField = jsonMessage.get(rule.getFieldName()).getAsString();
                boolean checkRes = filterFunctionsProcesses.get(rule.getFilterFunctionName()).test(curMessageField, rule.getFilterValue());
                message.setFilterState(checkRes);
            } else {
                message.setFilterState(false);
            }
            if (!message.isFilterState()) {
                break;
            }
        }
        return message;
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (message == null) {
            return null;
        } else if (message.getValue() == null) {
            message.setFilterState(false);
            return message;
        } else {
            try {
                JsonObject jsonMessage = JsonParser.parseString(message.getValue()).getAsJsonObject();
                Map<Long, List<Rule>> groupRules = Arrays.stream(rules).collect(Collectors.groupingBy(Rule::getRuleId));
                for (Map.Entry<Long, List<Rule>> groupedRule : groupRules.entrySet()) {
                    if (!processMessage(message, jsonMessage, groupedRule.getValue()).isFilterState()) {
                        break;
                    }
                }
            } catch (Exception ex) {
                log.error("An error occurred while processing message");
                message.setFilterState(false);
            }
            return message;
        }
    }
}
