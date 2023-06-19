package ru.mai.lessons.rpks.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.NonNull;
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
            message.setFilterState(true); // вот тут момент, что можем пропустить, можем нет. Как правильно не знаю.
        } else {
            Map<Long, List<Rule>> groupRules = Arrays.stream(rules).collect(Collectors.groupingBy(Rule::getRuleId));
            for (Map.Entry<Long, List<Rule>> groupedRule : groupRules.entrySet()) {
                if (!processMessage(message, groupedRule.getValue())) {
                    break;
                }
            }
        }
        return message;
    }

    private boolean processMessage(@NonNull Message message, List<Rule> rules) {
        boolean result = false;
        JsonObject messageInJson = JsonParser.parseString(message.getValue()).getAsJsonObject();
        for (Rule rule : rules) {
            if (messageInJson.has(rule.getFieldName())) {
                log.info(
                        "Check rule (ruleId : %d, field : %s, function : %s, value : %s) on message %s.".formatted(
                                rule.getRuleId(),
                                rule.getFieldName(),
                                rule.getFilterFunctionName(),
                                rule.getFilterValue(),
                                message.getValue())
                );
                if (rule.getFilterFunctionName().equals("equals")) {
                    message.setFilterState(messageInJson.get(rule.getFieldName()).toString().equals(rule.getFilterValue().toString()));
                    result = true;
                } else if (rule.getFilterFunctionName().equals("contains")) {
                    message.setFilterState(messageInJson.get(rule.getFieldName()).toString().contains(rule.getFilterValue().toString()));
                    result = true;
                } else if (rule.getFilterFunctionName().equals("not_equals")) {
                    message.setFilterState(!messageInJson.get(rule.getFieldName()).toString().equals(rule.getFilterValue().toString()));
                    result = true;
                } else if (rule.getFilterFunctionName().equals("not_contains")) {
                    message.setFilterState(!messageInJson.get(rule.getFieldName()).toString().contains(rule.getFilterValue().toString()));
                    result = true;
                }
            }
            if (message.isFilterState()) {
                break;
            }
        }
        return result;
    }
}
