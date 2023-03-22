package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;


@Slf4j
public class MyRuleProcessor implements RuleProcessor {

    private boolean compare(Rule rule, JsonNode jsonNode){
        switch (rule.getFilterFunctionName()) {
            case "equals" -> {
                if (!jsonNode.get(rule.getFieldName()).asText().equals(rule.getFilterValue())) {
                    return false;
                }
            }
            case "not_equals" -> {
                if (jsonNode.get(rule.getFieldName()).asText().equals(rule.getFilterValue())) {
                    return false;
                }
            }
            case "contains" -> {
                if (!jsonNode.get(rule.getFieldName()).asText().contains(rule.getFilterValue())) {
                    return false;
                }
            }
            case "not_contains" -> {
                if (jsonNode.get(rule.getFieldName()).asText().contains(rule.getFilterValue())) {
                    return false;
                }
            }
            default -> {
                log.error("error with FilterFunctionName");
                return false;
            }
        }

        return true;
    }
    @Override
    public Message processing(Message message, Rule[] rules) {
        try {
            if (message.getValue().isEmpty() || rules.length == 0){
                message.setFilterState(false);
                return message;
            }
            String value = message.getValue();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(value);

            for (Rule rule : rules) {
                if (jsonNode.get(rule.getFieldName()) == null){
                    message.setFilterState(false);
                    break;
                }
                log.info(rule.getFilterFunctionName() + " : " + rule.getFieldName() + " : " + rule.getFilterValue() + " : " + jsonNode.get(rule.getFieldName()).asText());
                //equals, contains, not_equals, not_contains
                if(!compare(rule, jsonNode)){
                    message.setFilterState(false);
                    return message;
                }
            }
        } catch (JsonProcessingException e) {
            log.error("i have exception for you");
            message.setFilterState(false);
        }
        return message;
    }
}
