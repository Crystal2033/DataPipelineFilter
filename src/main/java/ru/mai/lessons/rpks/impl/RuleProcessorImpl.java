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
import java.util.stream.Collectors;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    @Override
    public Message processing(Message message, Rule[] rules) {
        if (message == null) {
            log.info("Passed null message.");
        } else if (rules == null || rules.length == 0) {
            log.info("Passed message %s with no rules".formatted(message.getValue()));
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

    private void processMessage(Message message, List<Rule> rules) {
        try {
            JsonObject messageInJson = JsonParser.parseString(message.getValue()).getAsJsonObject();
            for (Rule rule : rules) {
                if (messageInJson.has(rule.getFieldName())) {
                    log.info("Check rule (ruleId : %d, field : %s, function : %s, value : %s) on message %s.".formatted(rule.getRuleId(), rule.getFieldName(), rule.getFilterFunctionName(), rule.getFilterValue(), message.getValue()));
                    switch (rule.getFilterFunctionName()) {
                        case "equals" ->
                                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && messageInJson.get(rule.getFieldName()).getAsString().equals(rule.getFilterValue()));
                        case "contains" ->
                                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && messageInJson.get(rule.getFieldName()).getAsString().contains(rule.getFilterValue()));
                        case "not_equals" ->
                                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && !messageInJson.get(rule.getFieldName()).getAsString().equals(rule.getFilterValue()));
                        case "not_contains" ->
                                message.setFilterState(!messageInJson.get(rule.getFieldName()).isJsonNull() && !messageInJson.get(rule.getFieldName()).getAsString().contains(rule.getFilterValue()));
                        default -> log.error("Unexpected rule filter function name.");
                    }
                } else {
                    message.setFilterState(false);
                }
                if (!message.isFilterState()) {
                    return;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
