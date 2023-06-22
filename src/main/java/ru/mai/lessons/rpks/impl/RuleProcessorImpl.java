package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
    private final ObjectMapper mapper = new ObjectMapper();

    private enum FilterFunction {
        EQUALS,
        NOT_EQUALS,
        CONTAINS,
        NOT_CONTAINS
    }

    @Override
    public Message processing(Message message, Rule[] rules) {
        message.setFilterState(true);
        try {
            JsonNode jsonNode = mapper.readTree(message.getValue());

            if (rules == null || rules.length == 0) {
                message.setFilterState(false);
            } else {
                for (Rule rule : rules) {
                    if (!processRule(rule, jsonNode)) {
                        message.setFilterState(false);
                        break;
                    }
                }
            }
        } catch (JsonProcessingException e) {
            log.error("JSON processing exception: {}", e.toString());
            message.setFilterState(false);
        } catch (Exception e) {
            log.error("Exception occurred: {}", e.toString());
            message.setFilterState(false);
        }
        return message;
    }

    private boolean processRule(Rule rule, JsonNode jsonNode) {
        JsonNode fieldNode = jsonNode.get(rule.getFieldName());
        if (fieldNode == null || !fieldNode.isValueNode()) {
            return false;
        }

        String fieldValue = fieldNode.asText();
        return checkRule(rule, fieldValue);
    }

    private boolean checkRule(Rule rule, String fieldValue) {
        FilterFunction filterFunction = FilterFunction.valueOf(rule.getFilterFunctionName().toUpperCase());
        String filterValue = rule.getFilterValue();

        return switch (filterFunction) {
            case EQUALS -> fieldValue.equals(filterValue);
            case NOT_EQUALS -> !fieldValue.equals(filterValue);
            case CONTAINS -> fieldValue.contains(filterValue);
            case NOT_CONTAINS -> !fieldValue.contains(filterValue);
        };
    }
}
