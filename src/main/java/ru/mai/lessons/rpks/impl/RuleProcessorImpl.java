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
    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules == null || rules.length == 0) {
            message.setFilterState(false);
            return message;
        }

        for (Rule rule : rules) {

            String value = "";
            ObjectMapper mapper = new ObjectMapper();
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
        return switch (rule) {
            case "equals" -> value.equals(checkValue);
            case "contains" -> value != null && value.contains(checkValue);
            case "not_equals" -> value != null && !value.equals("") && (!value.equals(checkValue));
            case "not_contains" -> value != null && !value.equals("") &&(!value.contains(checkValue));
            default -> false;
        };

    }
}
