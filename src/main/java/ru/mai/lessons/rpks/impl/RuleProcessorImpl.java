package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;


@RequiredArgsConstructor
public final class RuleProcessorImpl implements RuleProcessor {
    public enum rulesMode {
        EQUALS, NOT_EQUALS, CONTAINS, NOT_CONTAINS
    }
    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setFilterState(false);
            return message;
        }
        ObjectMapper mapper = new ObjectMapper();

        for (Rule rule : rules) {

            String value = "";
            JsonNode node;

            try {
                node = mapper.readTree(message.getValue());
                if (node.path(rule.getFieldName()) != null) {
                    value = node.path(rule.getFieldName()).asText();
                }else value = "";
            } catch (JsonProcessingException e) {
                message.setFilterState(false);
            }

            boolean messageState = ruleChecker(value, rule.getFilterValue(), rule.getFilterFunctionName());
            message.setFilterState(messageState);

            if (!messageState) {
                break;
            }
        }
        return message;
    }

    private boolean ruleChecker(String value, String checkValue, String rule) {
        rulesMode ruleMode = rulesMode.valueOf(rule.toUpperCase());
        return switch (ruleMode) {
            case EQUALS -> value.equals(checkValue);
            case CONTAINS -> value != null && value.contains(checkValue);
            case NOT_EQUALS -> value != null && !value.equals("") && (!value.equals(checkValue));
            case NOT_CONTAINS -> value != null && !value.equals("") &&(!value.contains(checkValue));
        };

    }
}
