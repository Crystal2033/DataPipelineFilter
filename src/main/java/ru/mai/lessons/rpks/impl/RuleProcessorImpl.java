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
    public static class FilterFunctions {
        private FilterFunctions() {
        }

        public static final String EQUALS_OPERATION = "EQUALS";
        public static final String NOT_EQUALS = "NOT_EQUALS";
        public static final String CONTAINS = "CONTAINS";
        public static final String NOT_CONTAINS = "NOT_CONTAINS";
    }

    private final Map<String, BiPredicate<String, String>> filterFunctionsProcesses = Map.of(
            FilterFunctions.EQUALS_OPERATION, String::equals,
            FilterFunctions.NOT_EQUALS, (messageValue, filterValue) -> !messageValue.equals(filterValue),
            FilterFunctions.CONTAINS, String::contains,
            FilterFunctions.NOT_CONTAINS, (messageValue, filterValue) -> !messageValue.contains(filterValue));


    private Message processMessage(Message message, JsonObject jsonMessage, List<Rule> groupedRules) {
        for (Rule rule : groupedRules) {
            if (jsonMessage.has(rule.getFieldName())) {
                log.info("Check rule (field : %s, function : %s, value : %s)".formatted(rule.getFieldName(), rule.getFilterFunctionName(),
                        rule.getFilterValue()));
                String curMessageField = jsonMessage.get(rule.getFieldName()).getAsString();
                log.info("Checked message field value : " + curMessageField);
                boolean checkRes = filterFunctionsProcesses.get(rule.getFilterFunctionName()).test(curMessageField, rule.getFilterValue());
                message.setFilterState(checkRes);
            } else {
                log.info("Message does not contain field " + rule.getFieldName());
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
            log.info("Passed null message");
            return null;
        } else if (message.getValue() == null) {
            log.info("Passed message has null value");
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
                log.info("An error occurred while processing message");
                message.setFilterState(false);
            }
            return message;
        }
    }
}
