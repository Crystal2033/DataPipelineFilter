package ru.mai.lessons.rpks.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {

    @Getter
    public enum FilterFunction {
         EQUALS("equals"),
         CONTAINS("contains"),
         NOT_EQUALS("not_equals"),
         NOT_CONTAINS("not_contains");
        private final String title;
        FilterFunction(String title) {
            this.title = title;
        }
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        if (message == null) {
            log.debug("Passed null message.");
        } else if (rules == null || rules.length == 0) {
            log.debug("Passed message %s with no rules".formatted(message.getValue()));
            message.setFilterState(false);
        } else {
            Map<Long, List<Rule>> groupRules = Arrays.stream(rules).collect(Collectors.groupingBy(Rule::getRuleId));
            for (Map.Entry<Long, List<Rule>> groupedRule : groupRules.entrySet()) {
                processMessage(message, groupedRule.getValue());
                if (!message.isFilterState()) {
                    return message;
                }
            }
        }
        return message;
    }

    private void setFilterStateOnMessage(Message message, Rule rule, JsonObject messageInJson) {
        if (messageInJson.has(rule.getFieldName())) {
            log.debug("Check rule (ruleId : %d, field : %s, function : %s, value : %s) on message %s.".formatted(rule.getRuleId(), rule.getFieldName(), rule.getFilterFunctionName(), rule.getFilterValue(), message.getValue()));
            if (rule.getFilterFunctionName().equals(FilterFunction.EQUALS.getTitle())) {
                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && messageInJson.get(rule.getFieldName()).getAsString().equals(rule.getFilterValue()));
            } else if (rule.getFilterFunctionName().equals(FilterFunction.NOT_EQUALS.getTitle())) {
                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && !messageInJson.get(rule.getFieldName()).getAsString().equals(rule.getFilterValue()));
            } else if (rule.getFilterFunctionName().equals(FilterFunction.CONTAINS.getTitle())) {
                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && messageInJson.get(rule.getFieldName()).getAsString().contains(rule.getFilterValue()));
            } else if (rule.getFilterFunctionName().equals(FilterFunction.NOT_CONTAINS.getTitle())) {
                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && !messageInJson.get(rule.getFieldName()).getAsString().contains(rule.getFilterValue()));
            }
        } else {
            message.setFilterState(false);
        }
    }

    private void processMessage(Message message, List<Rule> rules) {
        try {
            JsonObject messageInJson = JsonParser.parseString(message.getValue()).getAsJsonObject();
            for (Rule rule : rules) {
                setFilterStateOnMessage(message, rule, messageInJson);
                if (!message.isFilterState()) {
                    return;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
