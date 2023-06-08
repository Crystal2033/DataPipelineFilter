package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

@Slf4j
public class RuleProcessorI implements RuleProcessor {
    public enum FilterFunction{
        EQUALS, CONTAINS, NOT_EQUALS, NOT_CONTAINS
    }
    @Override
    public Message processing(Message message, Rule[] rules) {
        if (rules.length == 0) {
            message.setFilterState(false);
            return message;
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(message.getValue());
            for (Rule rule :rules) {
                boolean res = false;
                String msgVal = node.path(rule.getFieldName()).asText();
                log.debug("in: " + msgVal + " ruleval: " + rule.getFilterValue() + " rul: " + rule.getFilterFunctionName() + "\n");
                if (!msgVal.isBlank()) {
                    res = check(msgVal, rule.getFilterValue(), rule.getFilterFunctionName());
                }
                message.setFilterState(res);
                if (!res) {
                    break;
                }
            }
        } catch (JsonProcessingException e) {
            message.setFilterState(false);
        }
        return message;
    }
    private boolean check(String value, String ruleVal, String rule){
        FilterFunction rules = FilterFunction.valueOf(rule.toUpperCase());
        if (rules.equals(FilterFunction.EQUALS)){
            return value.equals(ruleVal);
        }
        if (rules.equals(FilterFunction.CONTAINS)){
            return value.contains(ruleVal);
        }
        if (rules.equals(FilterFunction.NOT_EQUALS)){
            return (!value.equals(ruleVal));
        }
        if (rules.equals(FilterFunction.NOT_CONTAINS)){
            return !value.contains(ruleVal);
        }
        return false;
    }
}
